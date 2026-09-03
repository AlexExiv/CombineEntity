//
//  REPaginatorObservableCollectionExtra.swift
//  CombineEntity
//
//  Created by ALEXEY ABDULIN on 10/02/2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation
import Combine

public let PAGINATOR_END = -1000

public struct REPageParams<Extra, CollectionExtra>
{
    /// Current page
    public let page: Int
    /// Number of element per page
    public let perPage: Int
    /// Update requested from refreshing
    public let refreshing: Bool
    public let resetCache: Bool
    /// The first loading request flag
    public let first: Bool
    /// The observable's extra params for example filter and e.g.
    public let extra: Extra?
    /// The collection extra params it maybe a region or city or so on
    public let collectionExtra: CollectionExtra?
    
    init( page: Int, perPage: Int, refreshing: Bool = false, resetCache: Bool = false, first: Bool = false, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil )
    {
        self.page = page
        self.perPage = perPage
        self.refreshing = refreshing
        self.resetCache = resetCache
        self.first = first
        self.extra = extra
        self.collectionExtra = collectionExtra
    }
}

public class REPaginatorObservableCollectionExtra<Entity: REEntity, Extra, CollectionExtra>: REPaginatorObservableExtra<Entity, Extra>
{
    public typealias PageFetchCallback<PageExtra, PageCollectionExtra> = (REPageParams<PageExtra, PageCollectionExtra>) -> RESingle<[Entity]>
    public typealias PageFetchBackCallback<PageExtra, PageCollectionExtra> = (REPageParams<PageExtra, PageCollectionExtra>) -> RESingle<[any REBackEntityProtocol]>
    
    let rxPage = PassthroughSubject<REPageParams<Extra, CollectionExtra>, Never>()

    public private(set) var collectionExtra: CollectionExtra? = nil
    var started = false
    
    init( holder: REEntityCollection<Entity>, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil, perPage: Int = RE_ARRAY_PER_PAGE, start: Bool = true, observeOn: DispatchQueue, fetch: @escaping PageFetchCallback<Extra, CollectionExtra> )
    {
        self.collectionExtra = collectionExtra
        super.init( holder: holder, extra: extra, perPage: perPage, observeOn: observeOn )
        
        rxPage
            .combineLatest( rxSuspended )
            .filter { $0.0.page >= 0 && !$0.1 }
            .map { $0.0 }
            .handleEvents( receiveOutput:
            {
                [weak self] params in
                
                self?.rxLoader.send( params.first ? .firstLoading : .loading )
                self?.rxLastError.send( nil )
            } )
            .map
            {
                [weak self] params -> AnyPublisher<[Entity], Never> in
                
                guard let self else { return Just( [] ).eraseToAnyPublisher() }
                
                return fetch( params )
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
                self?.Set( entities: self?.Append( entities: $0 ) ?? [] )
            }
            .store( in: &cancellables )
        
        if start
        {
            started = true
            Request( REPageParams( page: 0, perPage: perPage, first: true, extra: extra, collectionExtra: collectionExtra ) )
        }
    }
    
    convenience init( holder: REEntityCollection<Entity>, initial: [Entity], collectionExtra: CollectionExtra? = nil, observeOn: DispatchQueue, fetch: @escaping PageFetchCallback<Extra, CollectionExtra> )
    {
        self.init( holder: holder, collectionExtra: collectionExtra, start: false, observeOn: observeOn, fetch: fetch )
        
        collection?.RxRequestForCombine( source: uuid, entities: initial )
            .receive( on: observeOn )
            .sink( receiveCompletion: { _ in }, receiveValue:
            {
                [weak self] in
                
                self?.Set( entities: $0 )
            } )
            .store( in: &cancellables )
        
        started = true
    }
    
    convenience init( holder: REEntityCollection<Entity>, extra: Extra? = nil, collectionExtra: CollectionExtra? = nil, perPage: Int = RE_ARRAY_PER_PAGE, start: Bool = true, observeOn: DispatchQueue, fetch: @escaping PageFetchBackCallback<Extra, CollectionExtra> )
    {
        self.init( holder: holder, extra: extra, collectionExtra: collectionExtra, perPage: perPage, start: start, observeOn: observeOn, fetch: { fetch( $0 ).map { $0.map { Entity( entity: $0 ) } }.eraseToAnyPublisher() } )
    }
    
    convenience init( holder: REEntityCollection<Entity>, initial: [Entity], collectionExtra: CollectionExtra? = nil, observeOn: DispatchQueue, fetch: @escaping PageFetchBackCallback<Extra, CollectionExtra> )
    {
        self.init( holder: holder, initial: initial, collectionExtra: collectionExtra, observeOn: observeOn, fetch: { fetch( $0 ).map { $0.map { Entity( entity: $0 ) } }.eraseToAnyPublisher() } )
    }
    
    private func Request( _ params: REPageParams<Extra, CollectionExtra> )
    {
        rxPage.send( params )
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
    
    public override func Next()
    {
        if rxLoader.value == .firstLoading || rxLoader.value == .loading
        {
            return
        }
        
        if started
        {
            Request( REPageParams( page: page + 1, perPage: perPage, extra: extra, collectionExtra: collectionExtra ) )
        }
        else
        {
            Refresh()
        }
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
        Request( REPageParams( page: page + 1, perPage: perPage, refreshing: true, resetCache: resetCache, first: !started, extra: self.extra, collectionExtra: self.collectionExtra ) )
        started = true
    }
}
