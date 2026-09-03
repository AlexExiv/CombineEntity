//
//  REKeyArrayObservableCollectionExtra.swift
//  CombineEntity
//
//  Created by ALEXEY ABDULIN on 07.10.2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation
import Combine

public struct REKeyParams<Key: Hashable & Sendable, Extra, CollectionExtra>
{
    /// Update requested from refreshing
    public let refreshing: Bool
    public let resetCache: Bool
    /// The first loading request flag
    public let first: Bool
    /// The keys of requested elements
    public let keys: [Key]
    /// The observable's extra params for example filter and e.g.
    public let extra: Extra?
    /// The collection extra params it maybe a region or city or so on
    public let collectionExtra: CollectionExtra?
    
    init( refreshing: Bool = false, resetCache: Bool = false, first: Bool = false, keys: [Key], extra: Extra? = nil, collectionExtra: CollectionExtra? = nil )
    {
        self.refreshing = refreshing
        self.resetCache = resetCache
        self.first = first
        self.keys = keys
        self.extra = extra
        self.collectionExtra = collectionExtra
    }
}

public class REKeyArrayObservableCollectionExtra<Entity: REEntity, Extra, CollectionExtra>: REKeyArrayObservableExtra<Entity, Extra>
{
    public typealias ArrayFetchCallback<KeyExtra, KeyCollectionExtra> = (REKeyParams<Entity.ID, KeyExtra, KeyCollectionExtra>) -> RESingle<[Entity]>
    public typealias ArrayFetchBackCallback<KeyExtra, KeyCollectionExtra> = (REKeyParams<Entity.ID, KeyExtra, KeyCollectionExtra>) -> RESingle<[any REBackEntityProtocol]>
    
    override var innerKeys: [Entity.ID]
    {
        set
        {
            lock.lock()
            defer { lock.unlock() }
            
            super.innerKeys = newValue
            Request( REKeyParams( keys: keys, extra: extra, collectionExtra: collectionExtra ) )
        }
        get
        {
            super.innerKeys
        }
    }
    
    let rxKeys = PassthroughSubject<REKeyParams<Entity.ID, Extra, CollectionExtra>, Never>()

    public private(set) var collectionExtra: CollectionExtra? = nil
    
    init( holder: REEntityCollection<Entity>, keys: [Entity.ID] = [], start: Bool = true, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil, observeOn: DispatchQueue, fetch: @escaping ArrayFetchCallback<Extra, CollectionExtra> )
    {
        self.collectionExtra = collectionExtra
        super.init( holder: holder, keys: keys, extra: extra, observeOn: observeOn )
        
        rxKeys
            .combineLatest( rxSuspended )
            .filter { $0.0.keys.count > 0 && !$0.1 }
            .map { $0.0 }
            .handleEvents( receiveOutput:
            {
                [weak self] params in
                
                self?.rxLoader.send( params.first ? .firstLoading : .loading )
                self?.rxLastError.send( nil )
            } )
            .receive( on: queue )
            .map
            {
                [weak self] params -> AnyPublisher<[Entity], Never> in
                
                guard let self else { return Just( [] ).eraseToAnyPublisher() }
                
                return self.RxFetchElements( params: params, fetch: fetch )
                    .catch
                    {
                        [weak self] error -> RESingle<[Entity]> in
                        
                        self?.rxError.send( error )
                        self?.rxLastError.send( error )
                        self?.rxLoader.send( .none )
                        return REJust( [] )
                    }
                    .flatMap
                    {
                        [weak self] in
                        
                        self?.collection?.RxRequestForCombine( source: self?.uuid ?? "", entities: $0 ) ?? REJust( [] )
                    }
                    .replaceError( with: [] )
                    .eraseToAnyPublisher()
            }
            .switchToLatest()
            .receive( on: observeOn )
            .sink
            {
                [weak self] in
                
                self?.rxLoader.send( .none )
                self?.Set( entities: $0 )
            }
            .store( in: &cancellables )
        
        rxKeys
            .filter { $0.keys.count == 0 }
            .receive( on: observeOn )
            .sink
            {
                [weak self] _ in
                
                self?.rxLoader.send( .none )
                self?.Set( entities: [] )
            }
            .store( in: &cancellables )
        
        if start
        {
            Request( REKeyParams( first: true, keys: keys, extra: extra, collectionExtra: collectionExtra ) )
        }
    }
    
    convenience init( holder: REEntityCollection<Entity>, initial: [Entity], collectionExtra: CollectionExtra? = nil, observeOn: DispatchQueue, fetch: @escaping ArrayFetchCallback<Extra, CollectionExtra> )
    {
        self.init( holder: holder, keys: initial.map { $0.id }, start: false, collectionExtra: collectionExtra, observeOn: observeOn, fetch: fetch )
        
        collection?.RxRequestForCombine( source: uuid, entities: initial )
            .receive( on: observeOn )
            .sink( receiveCompletion: { _ in }, receiveValue:
            {
                [weak self] in
                
                self?.Set( entities: $0 )
            } )
            .store( in: &cancellables )
    }
    
    convenience init( holder: REEntityCollection<Entity>, keys: [Entity.ID] = [], extra: Extra? = nil, collectionExtra: CollectionExtra? = nil, observeOn: DispatchQueue, fetch: @escaping ArrayFetchBackCallback<Extra, CollectionExtra> )
    {
        self.init( holder: holder, keys: keys, extra: extra, collectionExtra: collectionExtra, observeOn: observeOn, fetch: { fetch( $0 ).map { $0.map { Entity( entity: $0 ) } }.eraseToAnyPublisher() } )
    }
    
    convenience init( holder: REEntityCollection<Entity>, initial: [Entity], collectionExtra: CollectionExtra? = nil, observeOn: DispatchQueue, fetch: @escaping ArrayFetchBackCallback<Extra, CollectionExtra> )
    {
        self.init( holder: holder, initial: initial, collectionExtra: collectionExtra, observeOn: observeOn, fetch: { fetch( $0 ).map { $0.map { Entity( entity: $0 ) } }.eraseToAnyPublisher() } )
    }

    private func Request( _ params: REKeyParams<Entity.ID, Extra, CollectionExtra> )
    {
        rxKeys.send( params )
    }
    
    public override func Refresh( resetCache: Bool = false, extra: Extra? = nil )
    {
        CollectionRefresh( resetCache: resetCache, extra: extra )
    }
    
    override func _Refresh( resetCache: Bool = false, extra: Extra? = nil )
    {
        _CollectionRefresh( resetCache: resetCache, extra: extra )
    }

    override func RefreshData( resetCache: Bool, data: Any? )
    {
        _CollectionRefresh( resetCache: resetCache, collectionExtra: data as? CollectionExtra )
    }
    
    //MARK: - Collection
    func CollectionRefresh( resetCache: Bool = false, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil )
    {
        _CollectionRefresh( resetCache: resetCache, extra: extra, collectionExtra: collectionExtra )
    }
    
    func _CollectionRefresh( resetCache: Bool = false, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil )
    {
        lock.lock()
        defer { lock.unlock() }
        
        super._Refresh( resetCache: resetCache, extra: extra )
        self.collectionExtra = collectionExtra ?? self.collectionExtra
        Request( REKeyParams( refreshing: true, resetCache: resetCache, keys: keys, extra: self.extra, collectionExtra: self.collectionExtra ) )
    }
    
    //MARK: - Fetch
    private func RxFetchElements( params: REKeyParams<Entity.ID, Extra, CollectionExtra>, fetch: @escaping ArrayFetchCallback<Extra, CollectionExtra> ) -> RESingle<[Entity]>
    {
        if params.refreshing
        {
            return fetch( params )
        }
        
        let exist = params.keys.compactMap { k in collection?.sharedEntities[k] ?? entities.first( where: { k == $0.id } ) }
        if exist.count != params.keys.count
        {
            let existMap = exist.asEntitiesMap()
            let _params = REKeyParams( refreshing: params.refreshing, resetCache: params.resetCache, first: params.first, keys: params.keys.filter { existMap[$0] == nil }, extra: extra, collectionExtra: collectionExtra )
            return fetch( _params )
                .map
                {
                    let new = $0.asEntitiesMap()
                    REEntityCollectionConfig.Log( "KeyArray exist: \(existMap.count); new: \(new.count)" )
                    return params.keys.compactMap { existMap[$0] ?? new[$0] }
                }
                .eraseToAnyPublisher()
        }
        
        return REJust( exist )
    }
}
