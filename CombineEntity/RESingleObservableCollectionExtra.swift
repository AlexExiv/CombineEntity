//
//  RESingleObservableService.swift
//  CombineEntity
//
//  Created by ALEXEY ABDULIN on 10/02/2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation
import Combine

public struct RESingleParams<Entity: REEntity, Extra, CollectionExtra>
{
    /// Update requested from refreshing
    public let refreshing: Bool
    public let resetCache: Bool
    /// The first loading request flag
    public let first: Bool
    /// The key of the entity
    public let key: Entity.ID?
    public let lastEntity: Entity?
    /// The observable's extra params for example filter and so on which you've passed to the `Refresh` method or its analogues of `Observable` instance
    public let extra: Extra?
    /// The collection's global extra params it maybe a region or city or so on which you've passed to the `Refresh` method or its analogues of `Collection` of observables
    public let collectionExtra: CollectionExtra?
    
    init( refreshing: Bool = false, resetCache: Bool = false, first: Bool = false, key: Entity.ID?, lastEntity: Entity?, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil )
    {
        self.refreshing = refreshing
        self.resetCache = resetCache
        self.first = first
        self.key = key
        self.lastEntity = lastEntity
        self.extra = extra
        self.collectionExtra = collectionExtra
    }
}

///Represents Observable that contains only one element
///- Parameters:
///- `Entity`: Type of entity
///- `Extra`: Any extra type which passes to the `fetch` closure for using during the data fetching
///- `CollectionExtra`: Any collection's extra type which passes to the `fetch` closure for using during the data fetching
public class RESingleObservableCollectionExtra<Entity: REEntity, Extra, CollectionExtra>: RESingleObservableExtra<Entity, Extra>
{
    public typealias SingleFetchBackCallback = (RESingleParams<Entity, Extra, CollectionExtra>) -> RESingle<(any REBackEntityProtocol)?>
    public typealias SingleFetchCallback = (RESingleParams<Entity, Extra, CollectionExtra>) -> RESingle<Entity?>
    
    let _rxRefresh = CurrentValueSubject<RESingleParams<Entity, Extra, CollectionExtra>?, Never>( nil )
    public private(set) var collectionExtra: CollectionExtra? = nil
    var started = false
    
    private let fetchCallback: SingleFetchCallback
    private var refreshCancellable: AnyCancellable? = nil
    private var pendingParams: RESingleParams<Entity, Extra, CollectionExtra>? = nil
    
    public override var key: Entity.ID?
    {
        set
        {
            lock.lock()
            defer { lock.unlock() }
            
            super.key = newValue
            
            let params = _rxRefresh.value
            Request( RESingleParams( refreshing: true, resetCache: true, first: true, key: newValue, lastEntity: entity, extra: params?.extra, collectionExtra: params?.collectionExtra ) )
            started = true
        }
        get
        {
            super.key
        }
    }

    init( holder: REEntityCollection<Entity>, key: Entity.ID? = nil, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil, start: Bool = true, observeOn: DispatchQueue, fetch: @escaping SingleFetchCallback )
    {
        self.collectionExtra = collectionExtra
        self.fetchCallback = fetch
        
        super.init( holder: holder, key: key, extra: extra, observeOn: observeOn )
        
        if start
        {
            started = true
            Request( RESingleParams( first: true, key: key, lastEntity: entity, extra: extra, collectionExtra: collectionExtra ) )
        }
    }
    
    convenience init( holder: REEntityCollection<Entity>, initial: Entity, refresh: Bool, collectionExtra: CollectionExtra? = nil, observeOn: DispatchQueue, fetch: @escaping SingleFetchCallback )
    {
        self.init( holder: holder, key: initial.id, collectionExtra: collectionExtra, start: false, observeOn: observeOn, fetch: fetch )
        
        collection?.RxRequestForCombine( source: uuid, entity: initial )
            .receive( on: observeOn )
            .sink( receiveCompletion: { _ in }, receiveValue:
            {
                [weak self] in
                
                self?.rxPublish.send( $0 )
                self?.rxState.send( .ready )
            } )
            .store( in: &cancellables )
        
        started = !refresh
    }
    
    convenience init( holder: REEntityCollection<Entity>, initial: Entity, refresh: Bool, collectionExtra: CollectionExtra? = nil, observeOn: DispatchQueue, fetch: @escaping SingleFetchBackCallback )
    {
        self.init( holder: holder, initial: initial, refresh: refresh, collectionExtra: collectionExtra, observeOn: observeOn, fetch: { fetch( $0 ).map { $0 == nil ? nil : Entity( entity: $0! ) }.eraseToAnyPublisher() } )
    }
    
    convenience init( holder: REEntityCollection<Entity>, key: Entity.ID? = nil, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil, start: Bool = true, observeOn: DispatchQueue,  fetch: @escaping SingleFetchBackCallback )
    {
        self.init( holder: holder, key: key, extra: extra, collectionExtra: collectionExtra, start: start, observeOn: observeOn, fetch: { fetch( $0 ).map { $0 == nil ? nil : Entity( entity: $0! ) }.eraseToAnyPublisher() } )
    }
    
    private func Request( _ params: RESingleParams<Entity, Extra, CollectionExtra> )
    {
        _rxRefresh.send( params )
        pendingParams = params
        
        guard rxSuspended.value == false else { return }
        
        rxLoader.send( params.first ? .firstLoading : .loading )
        if params.first
        {
            rxState.send( .initializing )
        }
        rxLastError.send( nil )
        
        refreshCancellable?.cancel()
        refreshCancellable = fetchCallback( params )
            .flatMap
            {
                [weak self] entity -> RESingle<Entity?> in
                
                guard let self else { return REJust( nil ) }
                guard let entity else
                {
                    return REFail( NSError( domain: "", code: 404, userInfo: nil ) )
                }
                
                return self.collection?.RxRequestForCombine( source: self.uuid, entity: entity ).map { Optional( $0 ) }.eraseToAnyPublisher() ?? REJust( nil )
            }
            .receive( on: queue )
            .sink( receiveCompletion:
            {
                [weak self] in
                
                if case let .failure( error ) = $0
                {
                    self?.rxError.send( error )
                    self?.rxLastError.send( error )
                    self?.rxLoader.send( .none )
                    self?.rxPublish.send( nil )
                    self?.rxState.send( .notFound )
                }
            }, receiveValue:
            {
                [weak self] in
                
                self?.rxPublish.send( $0 )
                self?.rxLoader.send( .none )
                self?.rxState.send( $0 == nil ? .notFound : .ready )
            } )
    }
    
    override func ResumeData()
    {
        if let pendingParams
        {
            Request( pendingParams )
        }
    }
    
    override func _Refresh( resetCache: Bool = false, extra: Extra? = nil )
    {
        _CollectionRefresh( resetCache: resetCache, extra: extra )
    }
    
    override func RefreshData( resetCache: Bool, data: Any? )
    {
        _CollectionRefresh( resetCache: resetCache, collectionExtra: data as? CollectionExtra )
    }
    
    func CollectionRefresh( resetCache: Bool = false, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil )
    {
        _CollectionRefresh( resetCache: resetCache, extra: extra, collectionExtra: collectionExtra )
    }
    
    public override func Refresh( resetCache: Bool = false, extra: Extra? = nil )
    {
        CollectionRefresh( resetCache: resetCache, extra: extra )
    }
    
    func _CollectionRefresh( resetCache: Bool = false, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil )
    {
        lock.lock()
        defer { lock.unlock() }
        
        super._Refresh( resetCache: resetCache, extra: extra )
        self.collectionExtra = collectionExtra ?? self.collectionExtra
        Request( RESingleParams( refreshing: true, resetCache: resetCache, first: !started, key: key, lastEntity: entity, extra: self.extra, collectionExtra: self.collectionExtra ) )
        started = true
    }
}
