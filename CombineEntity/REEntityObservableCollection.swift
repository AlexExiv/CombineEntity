//
//  REEntityObservableCollection.swift
//  CombineEntity
//
//  Created by ALEXEY ABDULIN on 25/11/2019.
//  Copyright © 2019 ALEXEY ABDULIN. All rights reserved.
//

import Foundation
import Combine

public struct REEntityCollectionExtraParamsEmpty: Sendable
{
    
}

typealias TestMethod<E> = (E, Array<Any>) -> Bool
typealias CombineMethod<E> = (E, Array<Any>) -> E

struct RECombineSource<E>
{
    let sources: [AnyPublisher<Any, Never>]
    let test: TestMethod<E>
    let combine: CombineMethod<E>
}

/// Shared collection of entity obesrvales of particular `Entity` type that coordinates and manages observables updating
/// - Parameters:
///   - Entity: type of a `entity` managed by this collection
///   - CollectionExtra: `Extra` type that contains parameter or parameters that can cause process of reloading of all data related to this collection. This `Extra` passes to all fetch blocks and you can use it in all your requests
public class REEntityObservableCollectionExtra<Entity: REEntity, CollectionExtra>: REEntityCollection<Entity>
{
    public typealias SingleFetchBackCallback = RESingleObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>.SingleFetchBackCallback
    public typealias SingleExtraFetchBackCallback<Extra> = RESingleObservableCollectionExtra<Entity, Extra, CollectionExtra>.SingleFetchBackCallback
    
    public typealias SingleFetchCallback = RESingleObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>.SingleFetchCallback
    public typealias SingleExtraFetchCallback<Extra> = RESingleObservableCollectionExtra<Entity, Extra, CollectionExtra>.SingleFetchCallback
    
    public typealias KeyArrayFetchBackCallback = REKeyArrayObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>.ArrayFetchBackCallback<REEntityExtraParamsEmpty, CollectionExtra>
    public typealias KeyArrayExtraFetchBackCallback<Extra> = REKeyArrayObservableCollectionExtra<Entity, Extra, CollectionExtra>.ArrayFetchBackCallback<Extra, CollectionExtra>
    
    public typealias KeyArrayFetchCallback = REKeyArrayObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>.ArrayFetchCallback<REEntityExtraParamsEmpty, CollectionExtra>
    public typealias KeyArrayExtraFetchCallback<Extra> = REKeyArrayObservableCollectionExtra<Entity, Extra, CollectionExtra>.ArrayFetchCallback<Extra, CollectionExtra>
    
    public typealias PageFetchBackCallback = REPaginatorObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>.PageFetchBackCallback<REEntityExtraParamsEmpty, CollectionExtra>
    public typealias PageExtraFetchBackCallback<Extra> = REPaginatorObservableCollectionExtra<Entity, Extra, CollectionExtra>.PageFetchBackCallback<Extra, CollectionExtra>
    
    public typealias PageFetchCallback = REPaginatorObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>.PageFetchCallback<REEntityExtraParamsEmpty, CollectionExtra>
    public typealias PageExtraFetchCallback<Extra> = REPaginatorObservableCollectionExtra<Entity, Extra, CollectionExtra>.PageFetchCallback<Extra, CollectionExtra>
    
    public var repository: REEntityRepositoryProtocol?
    {
        set
        {
            lock.lock()
            defer { lock.unlock() }
            
            _repository = newValue
            repositoryCancellable?.cancel()
            repositoryCancellable = nil
            
            if let r = newValue
            {
                singleFetchBackCallback = { $0.key == nil ? REJust( nil ) : r._RxGet( key: REEntityKey( $0.key! ) ) }
                arrayFetchBackCallback = { r._RxGet( keys: $0.keys.map { REEntityKey( $0 ) } ) }
                
                if let ar = r as? REEntityAllRepositoryProtocol
                {
                    allArrayFetchCallback = { _ in ar._RxFetchAll() }
                }
                else
                {
                    allArrayFetchCallback = nil
                }
                
                repositoryCancellable = r
                    .rxEntitiesUpdated
                    .receive( on: queue )
                    .sink
                    {
                        [weak self] updates in
                        
                        self?.ApplyRepositoryUpdates( updates )
                    }
            }
            else
            {
                singleFetchBackCallback = nil
                arrayFetchBackCallback = nil
                allArrayFetchCallback = nil
            }
        }
        get
        {
            lock.lock()
            defer { lock.unlock() }
            
            return _repository
        }
    }
    var _repository: REEntityRepositoryProtocol? = nil
    var repositoryCancellable: AnyCancellable? = nil
    
    public var singleFetchBackCallback: SingleFetchBackCallback? = nil
    public var singleFetchCallback: SingleFetchCallback? = nil
    public var arrayFetchBackCallback: KeyArrayFetchBackCallback? = nil
    public var arrayFetchCallback: KeyArrayFetchCallback? = nil
    var allArrayFetchCallback: PageFetchBackCallback? = nil
    
    public private(set) var collectionExtra: CollectionExtra? = nil
    private(set) var combineSources = [RECombineSource<Entity>]()
    
    var combineCancellable: AnyCancellable? = nil
    
    public init( queue: DispatchQueue? = nil, collectionExtra: CollectionExtra? = nil )
    {
        self.collectionExtra = collectionExtra
        super.init( queue: queue ?? DispatchQueue( label: "observable.collection" ) )
    }

    public convenience init( operationQueue: OperationQueue, collectionExtra: CollectionExtra? = nil )
    {
        self.init( queue: DispatchQueue( label: operationQueue.name ?? "observable.collection.operationQueue" ), collectionExtra: collectionExtra )
    }
    
    deinit
    {
        combineCancellable?.cancel()
        repositoryCancellable?.cancel()
    }
    
    private func ApplyRepositoryUpdates( _ updates: [REEntityUpdated] )
    {
        lock.lock()
        defer { lock.unlock() }
        
        let keys = updates.filter { $0.entity == nil && $0.fieldPath == nil }
        let entities = updates.filter { $0.entity != nil && $0.fieldPath == nil }
        let indirect = updates.filter { $0.fieldPath != nil }
            .map
            {
                k in
                
                self.sharedEntities.values.filter { REEntityKey( $0[keyPath: k.fieldPath!] as! AnyHashable ) == k.key }.map { $0.id }
            }
            .flatMap { $0 }

        REEntityCollectionConfig.Log( "\(type( of: Entity.self )): Repository requested update: \(updates)" )
        REEntityCollectionConfig.Log( "\(type( of: Entity.self )): Total updates: KEYS - \(keys.count); ENTITIES - \(entities.count); INDIRECT - \(indirect.count)" )
        
        if keys.count == 1
        {
            self.Commit( repositoryKey: keys[0].key, operation: keys[0].operation )
        }
        else if keys.count > 1
        {
            self.Commit( repositoryKeys: keys.map { $0.key }, operations: keys.map { $0.operation } )
        }
        
        if indirect.count == 1
        {
            self.Commit( key: indirect[0], operation: .update )
        }
        else if indirect.count > 1
        {
            self.Commit( keys: indirect, operation: .update )
        }
        
        if entities.count > 0
        {
            self.Commit( entities: entities.map { $0.entity! }, operations: entities.map { $0.operation } )
        }
    }

    //MARK: - Single Observables
    /// Creates the `SingleObservable` connected to this collection
    /// - Parameters:
    ///   - key: start key of the single observable. Default is `nil`
    ///   - start: flag indicates start or not fetching fata immidiately after the object `init`. Default is `true`
    ///   - fetch: closure for the fetch request
    /// - Returns: single observable
    public func CreateSingleBack( key: Entity.ID? = nil, start: Bool = true, _ fetch: @escaping SingleFetchBackCallback ) -> RESingleObservable<Entity>
    {
        return RESingleObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, key: key, collectionExtra: collectionExtra, start: start, observeOn: queue, fetch: fetch )
    }

    public func CreateSingleBackExtra<Extra>( key: Entity.ID? = nil, extra: Extra? = nil, start: Bool = true, _ fetch: @escaping SingleExtraFetchBackCallback<Extra> ) -> RESingleObservableExtra<Entity, Extra>
    {
        return RESingleObservableCollectionExtra<Entity, Extra, CollectionExtra>( holder: self, key: key, extra: extra, collectionExtra: collectionExtra, start: start, observeOn: queue, fetch: fetch )
    }
    
    public func CreateSingle( key: Entity.ID? = nil, start: Bool = true, _ fetch: @escaping SingleFetchCallback ) -> RESingleObservable<Entity>
    {
        return RESingleObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, key: key, collectionExtra: collectionExtra, start: start, observeOn: queue, fetch: fetch )
    }

    public func CreateSingleExtra<Extra>( key: Entity.ID? = nil, extra: Extra? = nil, start: Bool = true, _ fetch: @escaping SingleExtraFetchCallback<Extra> ) -> RESingleObservableExtra<Entity, Extra>
    {
        return RESingleObservableCollectionExtra<Entity, Extra, CollectionExtra>( holder: self, key: key, extra: extra, collectionExtra: collectionExtra, start: start, observeOn: queue, fetch: fetch )
    }
    
    public override func CreateSingle( initial: Entity, refresh: Bool = false ) -> RESingleObservable<Entity>
    {
        if singleFetchCallback != nil
        {
            return RESingleObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, initial: initial, refresh: refresh, collectionExtra: collectionExtra, observeOn: queue, fetch: singleFetchCallback! )
        }
        else if singleFetchBackCallback != nil
        {
            return RESingleObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, initial: initial, refresh: refresh, collectionExtra: collectionExtra, observeOn: queue, fetch: singleFetchBackCallback! )
        }
        
        preconditionFailure( "To create Single with initial value you must specify singleFetchCallback or singleFetchBackCallback before" )
    }
    
    public func CreateSingle( key: Entity.ID? = nil, start: Bool = true, refresh: Bool = false ) -> RESingleObservable<Entity>
    {
        let e = key == nil ? nil : sharedEntities[key!]
        if e == nil
        {
            if singleFetchCallback != nil
            {
                return CreateSingle( key: key, start: key != nil && start, singleFetchCallback! )
            }
            else if singleFetchBackCallback != nil
            {
                return CreateSingleBack( key: key, start: key != nil && start, singleFetchBackCallback! )
            }
            
            preconditionFailure( "To create Single with key you must specify singleFetchCallback or singleFetchBackCallback before" )
        }
        
        return CreateSingle( initial: e!, refresh: refresh )
    }
    
    //MARK: - Array Observables
    public func CreateArrayBack( start: Bool = true, _ fetch: @escaping PageFetchBackCallback ) -> REArrayObservable<Entity>
    {
        return REPaginatorObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, collectionExtra: collectionExtra, start: start, observeOn: queue, fetch: fetch )
    }
    
    public func CreateArrayBackExtra<Extra>( extra: Extra? = nil, start: Bool = true, _ fetch: @escaping PageExtraFetchBackCallback<Extra> ) -> REArrayObservableExtra<Entity, Extra>
    {
        return REPaginatorObservableCollectionExtra<Entity, Extra, CollectionExtra>( holder: self, extra: extra, collectionExtra: collectionExtra, start: start, observeOn: queue, fetch: fetch )
    }
    
    public func CreateArrayBack() -> REArrayObservable<Entity>
    {
        if let af = allArrayFetchCallback
        {
            return REPaginatorObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, collectionExtra: collectionExtra, start: true, observeOn: queue, fetch: af )
        }
        
        preconditionFailure( "Repository doesn't conform to REEntityAllRepositoryProtocol" )
    }
    
    public func CreateArray( start: Bool = true, _ fetch: @escaping PageFetchCallback ) -> REArrayObservable<Entity>
    {
        return REPaginatorObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, collectionExtra: collectionExtra, start: start, observeOn: queue, fetch: fetch )
    }
    
    public func CreateArrayExtra<Extra>( extra: Extra? = nil, start: Bool = true, _ fetch: @escaping PageExtraFetchCallback<Extra> ) -> REArrayObservableExtra<Entity, Extra>
    {
        return REPaginatorObservableCollectionExtra<Entity, Extra, CollectionExtra>( holder: self, extra: extra, collectionExtra: collectionExtra, start: start, observeOn: queue, fetch: fetch )
    }
    
    //MARK: - Array Keys Observables
    public func CreateKeyArrayBack( keys: [Entity.ID] = [], _ fetch: @escaping KeyArrayFetchBackCallback ) -> REKeyArrayObservable<Entity>
    {
        return REKeyArrayObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, keys: keys, collectionExtra: collectionExtra, observeOn: queue, fetch: fetch )
    }
    
    public func CreateKeyArrayBackExtra<Extra>( keys: [Entity.ID] = [], extra: Extra? = nil, _ fetch: @escaping KeyArrayExtraFetchBackCallback<Extra> ) -> REKeyArrayObservableExtra<Entity, Extra>
    {
        return REKeyArrayObservableCollectionExtra<Entity, Extra, CollectionExtra>( holder: self, keys: keys, extra: extra, collectionExtra: collectionExtra, observeOn: queue, fetch: fetch )
    }
    
    public func CreateKeyArray( keys: [Entity.ID] = [], _ fetch: @escaping KeyArrayFetchCallback ) -> REKeyArrayObservable<Entity>
    {
        return REKeyArrayObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, keys: keys, collectionExtra: collectionExtra, observeOn: queue, fetch: fetch )
    }
    
    public func CreateKeyArrayExtra<Extra>( keys: [Entity.ID] = [], extra: Extra? = nil, _ fetch: @escaping KeyArrayExtraFetchCallback<Extra> ) -> REKeyArrayObservableExtra<Entity, Extra>
    {
        return REKeyArrayObservableCollectionExtra<Entity, Extra, CollectionExtra>( holder: self, keys: keys, extra: extra, collectionExtra: collectionExtra, observeOn: queue, fetch: fetch )
    }
    
    public override func CreateKeyArray( initial: [Entity] ) -> REKeyArrayObservable<Entity>
    {
        if arrayFetchCallback != nil
        {
            return REKeyArrayObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, initial: initial, collectionExtra: collectionExtra, observeOn: queue, fetch: arrayFetchCallback! )
        }
        else if arrayFetchBackCallback != nil
        {
            return REKeyArrayObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, initial: initial, collectionExtra: collectionExtra, observeOn: queue, fetch: arrayFetchBackCallback! )
        }
        
        preconditionFailure( "To create Array with initial values you must specify arrayFetchCallback or arrayFetchBackCallback before" )
    }
    
    public func CreateKeyArray( keys: [Entity.ID] ) -> REKeyArrayObservable<Entity>
    {
        if arrayFetchCallback != nil
        {
            return CreateKeyArray( keys: keys, arrayFetchCallback! )
        }
        else if arrayFetchBackCallback != nil
        {
            return CreateKeyArrayBack( keys: keys, arrayFetchBackCallback! )
        }
        
        preconditionFailure( "To create Array with initial values you must specify arrayFetchCallback or arrayFetchBackCallback before" )
    }
    
    //MARK: - Paginator Observables
    public func CreatePaginatorBack( perPage: Int = 35, start: Bool = true, _ fetch: @escaping PageFetchBackCallback ) -> REPaginatorObservable<Entity>
    {
        return REPaginatorObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, collectionExtra: collectionExtra, perPage: perPage, start: start, observeOn: queue, fetch: fetch )
    }
    
    public func CreatePaginatorBackExtra<Extra>( extra: Extra? = nil, perPage: Int = 35, start: Bool = true, _ fetch: @escaping PageExtraFetchBackCallback<Extra> ) -> REPaginatorObservableExtra<Entity, Extra>
    {
        return REPaginatorObservableCollectionExtra<Entity, Extra, CollectionExtra>( holder: self, extra: extra, collectionExtra: collectionExtra, perPage: perPage, start: start, observeOn: queue, fetch: fetch )
    }
    
    public func CreatePaginator( perPage: Int = 35, start: Bool = true, _ fetch: @escaping PageFetchCallback ) -> REPaginatorObservable<Entity>
    {
        return REPaginatorObservableCollectionExtra<Entity, REEntityExtraParamsEmpty, CollectionExtra>( holder: self, collectionExtra: collectionExtra, perPage: perPage, start: start, observeOn: queue, fetch: fetch )
    }
    
    public func CreatePaginatorExtra<Extra>( extra: Extra? = nil, perPage: Int = 35, start: Bool = true, _ fetch: @escaping PageExtraFetchCallback<Extra> ) -> REPaginatorObservableExtra<Entity, Extra>
    {
        return REPaginatorObservableCollectionExtra<Entity, Extra, CollectionExtra>( holder: self, extra: extra, collectionExtra: collectionExtra, perPage: perPage, start: start, observeOn: queue, fetch: fetch )
    }
    
    //MARK: - Combine Latest
    private func Source<O: Publisher>( _ source: O ) -> AnyPublisher<Any, Never>
    {
        source
            .map { $0 as Any }
            .catch { _ in Empty<Any, Never>() }
            .receive( on: queue )
            .eraseToAnyPublisher()
    }
    
    public func combineLatest<O: Publisher>( _ source: O, test: @escaping ((Entity, O.Output)) -> Bool = { _ in true }, apply: @escaping ((Entity, O.Output)) -> Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        combineSources.append( RECombineSource<Entity>( sources: [Source( source )], test: { (e, a) in test( (e, a[0] as! O.Output) ) }, combine: { (e, a) in apply( (e, a[0] as! O.Output) ) } ) )
        BuildCombines()
    }

    public func combineLatest<O0: Publisher, O1: Publisher>( _ source0: O0, _ source1: O1, _ test: @escaping ((Entity, O0.Output, O1.Output)) -> Bool = { _ in true }, apply: @escaping ((Entity, O0.Output, O1.Output)) -> Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        let sources = [Source( source0 ),
                       Source( source1 )]
        
        combineSources.append( RECombineSource<Entity>( sources: sources, test: { (e, a) in test( (e, a[0] as! O0.Output, a[1] as! O1.Output) ) }, combine: { (e, a) in apply( (e, a[0] as! O0.Output, a[1] as! O1.Output) ) } ) )
        BuildCombines()
    }

    public func combineLatest<O0: Publisher, O1: Publisher, O2: Publisher>( _ source0: O0, _ source1: O1, _ source2: O2, test: @escaping ((Entity, O0.Output, O1.Output, O2.Output)) -> Bool = { _ in true }, _ apply: @escaping ((Entity, O0.Output, O1.Output, O2.Output)) -> Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        let sources = [Source( source0 ),
                       Source( source1 ),
                       Source( source2 )]
        
        combineSources.append( RECombineSource<Entity>( sources: sources, test: { (e, a) in test( (e, a[0] as! O0.Output, a[1] as! O1.Output, a[2] as! O2.Output) ) }, combine: { (e, a) in apply( (e, a[0] as! O0.Output, a[1] as! O1.Output, a[2] as! O2.Output) ) } ) )
        BuildCombines()
    }

    public func combineLatest<O0: Publisher, O1: Publisher, O2: Publisher, O3: Publisher>( _ source0: O0, _ source1: O1, _ source2: O2, _ source3: O3, test: @escaping ((Entity, O0.Output, O1.Output, O2.Output, O3.Output)) -> Bool = { _ in true }, apply: @escaping ((Entity, O0.Output, O1.Output, O2.Output, O3.Output)) -> Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        let sources = [Source( source0 ),
                       Source( source1 ),
                       Source( source2 ),
                       Source( source3 )]
        
        combineSources.append( RECombineSource<Entity>( sources: sources, test: { (e, a) in test( (e, a[0] as! O0.Output, a[1] as! O1.Output, a[2] as! O2.Output, a[3] as! O3.Output) ) }, combine: { (e, a) in apply( (e, a[0] as! O0.Output, a[1] as! O1.Output, a[2] as! O2.Output, a[3] as! O3.Output) ) } ) )
        BuildCombines()
    }

    public func combineLatest<O0: Publisher, O1: Publisher, O2: Publisher, O3: Publisher, O4: Publisher>( _ source0: O0, _ source1: O1, _ source2: O2, _ source3: O3, _ source4: O4, test: @escaping ((Entity, O0.Output, O1.Output, O2.Output, O3.Output, O4.Output)) -> Bool = { _ in true }, apply: @escaping ((Entity, O0.Output, O1.Output, O2.Output, O3.Output, O4.Output)) -> Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        let sources = [Source( source0 ),
                       Source( source1 ),
                       Source( source2 ),
                       Source( source3 ),
                       Source( source4 )]
        
        combineSources.append( RECombineSource<Entity>( sources: sources, test: { (e, a) in test( (e, a[0] as! O0.Output, a[1] as! O1.Output, a[2] as! O2.Output, a[3] as! O3.Output, a[4] as! O4.Output) ) }, combine: { (e, a) in apply( (e, a[0] as! O0.Output, a[1] as! O1.Output, a[2] as! O2.Output, a[3] as! O3.Output, a[4] as! O4.Output) ) } ) )
        BuildCombines()
    }

    public func combineLatest<O0: Publisher, O1: Publisher, O2: Publisher, O3: Publisher, O4: Publisher, O5: Publisher>( _ source0: O0, _ source1: O1, _ source2: O2, _ source3: O3, _ source4: O4, _ source5: O5, _ test: @escaping ((Entity, O0.Output, O1.Output, O2.Output, O3.Output, O4.Output, O5.Output)) -> Bool = { _ in true }, apply: @escaping ((Entity, O0.Output, O1.Output, O2.Output, O3.Output, O4.Output, O5.Output)) -> Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        let sources = [Source( source0 ),
                       Source( source1 ),
                       Source( source2 ),
                       Source( source3 ),
                       Source( source4 ),
                       Source( source5 )]
        
        combineSources.append( RECombineSource<Entity>( sources: sources, test: { (e, a) in test( (e, a[0] as! O0.Output, a[1] as! O1.Output, a[2] as! O2.Output, a[3] as! O3.Output, a[4] as! O4.Output, a[5] as! O5.Output) ) }, combine: { (e, a) in apply( (e, a[0] as! O0.Output, a[1] as! O1.Output, a[2] as! O2.Output, a[3] as! O3.Output, a[4] as! O4.Output, a[5] as! O5.Output) ) } ) )
        BuildCombines()
    }
    
    override func RxRequestForCombine( source: String = "", entity: Entity, updateChilds: Bool = true ) -> RESingle<Entity>
    {
        lock.lock()
        let sources = combineSources
        lock.unlock()
        
        var publisher = REJust( entity )
        sources.forEach
        {
            ms in
            
            publisher = publisher
                .flatMap
                {
                    e in
                    
                    self.CombineSourceValues( ms.sources )
                        .prefix( 1 )
                        .setFailureType( to: Error.self )
                        .map { ms.test( e, $0 ) ? ms.combine( e, $0 ) : e }
                        .eraseToAnyPublisher()
                }
                .eraseToAnyPublisher()
        }
        
        return publisher
            .handleEvents( receiveOutput:
            {
                if updateChilds
                {
                    self.Update( source: source, entity: $0 )
                }
            } )
            .eraseToAnyPublisher()
    }
    
    override func RxRequestForCombine( source: String = "", entities: [Entity], updateChilds: Bool = true ) -> RESingle<[Entity]>
    {
        lock.lock()
        let sources = combineSources
        lock.unlock()
        
        var publisher = REJust( entities )
        sources.forEach
        {
            ms in
            
            publisher = publisher
                .flatMap
                {
                    es in
                    
                    self.CombineSourceValues( ms.sources )
                        .prefix( 1 )
                        .setFailureType( to: Error.self )
                        .map { values in es.map { ms.test( $0, values ) ? ms.combine( $0, values ) : $0 } }
                        .eraseToAnyPublisher()
                }
                .eraseToAnyPublisher()
        }
        
        return publisher
            .handleEvents( receiveOutput:
            {
                if updateChilds
                {
                    self.Update( source: source, entities: $0 )
                }
            } )
            .eraseToAnyPublisher()
    }
    
    private func CombineSourceValues( _ sources: [AnyPublisher<Any, Never>] ) -> AnyPublisher<[Any], Never>
    {
        switch sources.count
        {
        case 0:
            return Just( [] ).eraseToAnyPublisher()
            
        case 1:
            return sources[0]
                .map { [$0] }
                .eraseToAnyPublisher()
            
        case 2:
            return Publishers.CombineLatest( sources[0], sources[1] )
                .map { [$0, $1] }
                .eraseToAnyPublisher()
            
        case 3:
            return Publishers.CombineLatest3( sources[0], sources[1], sources[2] )
                .map { [$0, $1, $2] }
                .eraseToAnyPublisher()
            
        case 4:
            return Publishers.CombineLatest4( sources[0], sources[1], sources[2], sources[3] )
                .map { [$0, $1, $2, $3] }
                .eraseToAnyPublisher()
            
        case 5:
            return Publishers.CombineLatest( CombineSourceValues( Array( sources[0..<4] ) ), sources[4] )
                .map { $0 + [$1] }
                .eraseToAnyPublisher()
            
        case 6:
            return Publishers.CombineLatest( CombineSourceValues( Array( sources[0..<4] ) ), CombineSourceValues( Array( sources[4..<6] ) ) )
                .map { $0 + $1 }
                .eraseToAnyPublisher()
            
        default:
            assertionFailure( "Unsupported number of the sources" )
            return Empty( completeImmediately: false ).eraseToAnyPublisher()
        }
    }
    
    func BuildCombines()
    {
        var obs: AnyPublisher<[(test: TestMethod<Entity>, combine: CombineMethod<Entity>, values: [Any])], Never> = Just( [] ).eraseToAnyPublisher()
        combineSources.forEach
        {
            ms in
            
            obs = Publishers.CombineLatest( obs, CombineSourceValues( ms.sources ) )
                .map { arr, values in arr + [(ms.test, ms.combine, values)] }
                .eraseToAnyPublisher()
        }
        
        combineCancellable?.cancel()
        combineCancellable = obs
            .sink
            {
                [weak self] in
                
                self?.ApplyCombines( combines: $0 )
            }
    }
    
    func ApplyCombines( combines: [(test: TestMethod<Entity>, combine: CombineMethod<Entity>, values: [Any])] )
    {
        lock.lock()
        defer { lock.unlock() }
        
        var toUpdate = [Entity.ID: Entity]()
        
        sharedEntities.keys.forEach
        {
            var e = sharedEntities[$0]!
            var updated = false
            for c in combines
            {
                if c.test( e, c.values )
                {
                    updated = true
                    e = c.combine( e, c.values )
                }
            }
            
            if updated
            {
                sharedEntities[$0] = e
                toUpdate[$0] = e
            }
        }
        
        if !toUpdate.isEmpty
        {
            items.forEach { $0.ref?.Update( source: "", entities: toUpdate ) }
        }
    }
    
    //MARK: - Commit
    public override func Commit( entity: Entity, operation: REUpdateOperation = .update )
    {
        Commit( entities: [entity], operation: operation )
    }

    public override func Commit( key: Entity.ID, operation: REUpdateOperation = .update )
    {
        switch operation
        {
        case .delete:
            CommitDelete( keys: Set( arrayLiteral: key ) )
            
        case .clear:
            CommitClear()
            
        default:
            Commit( repositoryKey: REEntityKey( key ), operation: operation )
        }
    }
    
    private func Commit( repositoryKey key: REEntityKey, operation: REUpdateOperation )
    {
        switch operation
        {
        case .delete:
            CommitDelete( keys: EntityKeys( repositoryKeys: Set( arrayLiteral: key ) ) )
            
        case .clear:
            CommitClear()
            
        default:
            if let r = repository
            {
                r._RxGet( key: key )
                    .flatMap
                    {
                        $0 == nil ? REJust( nil ) : self.RxRequestForCombine( source: "", entity: Entity( entity: $0! ), updateChilds: false ).map { Optional( $0 ) }.eraseToAnyPublisher()
                    }
                    .receive( on: queue )
                    .sink( receiveCompletion: { _ in }, receiveValue:
                    {
                        if let e = $0
                        {
                            self.Commit( entity: e, operation: operation )
                        }
                    } )
                    .store( in: &cancellables )
            }
            else
            {
                preconditionFailure( "To create Single with key you must specify singleFetchCallback or singleFetchBackCallback before" )
            }
        }
    }
    
    public override func Commit( key: Entity.ID, changes: (Entity) -> Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        if let e = sharedEntities[key]
        {
            let new = changes( e )
            sharedEntities[key] = new
            items.forEach { $0.ref?.Update( entity: new, operation: .update ) }
        }
    }
    
    public override func Commit( entities: [Entity], operation: REUpdateOperation = .update )
    {
        switch operation
        {
        case .delete:
            CommitDelete( keys: Set( entities.map { $0.id } ) )
            
        case .clear:
            CommitClear()
            
        default:
            RxRequestForCombine( entities: entities, updateChilds: false )
                .receive( on: queue )
                .sink( receiveCompletion: { _ in }, receiveValue:
                {
                    enities in
                    
                    self.lock.lock()
                    defer { self.lock.unlock() }
                    
                    var forUpdate = [Entity.ID: Entity]()
                    enities.forEach
                    {
                        forUpdate[$0.id] = $0
                        self.sharedEntities[$0.id] = $0
                    }
                    
                    self.items.forEach { $0.ref?.Update( entities: forUpdate, operation: operation ) }
                } )
                .store( in: &cancellables )
        }
    }
    
    public override func Commit( entities: [Entity], operations: [REUpdateOperation] )
    {
        let deleteKeys = Set( entities.enumerated().filter { operations[$0.0] == .delete }.map { $0.1.id } )
        let otherEntities = entities.enumerated().filter { operations[$0.0] != .delete }.map { $0.1 }
        let otherOpers = operations.filter { $0 != .delete }
        
        if let _ = operations.first( where: { $0 == .clear } )
        {
            CommitClear()
        }
        
        CommitDelete( keys: deleteKeys )

        RxRequestForCombine( entities: otherEntities, updateChilds: false )
            .receive( on: queue )
            .sink( receiveCompletion: { _ in }, receiveValue:
            {
                enities in
                
                self.lock.lock()
                defer { self.lock.unlock() }
                
                var forUpdate = [Entity.ID: Entity]()
                var operationUpdate = [Entity.ID: REUpdateOperation]()
                enities.enumerated().forEach
                {
                    forUpdate[$1.id] = $1
                    operationUpdate[$1.id] = otherOpers[$0]
                    self.sharedEntities[$1.id] = $1
                }
                
                self.items.forEach { $0.ref?.Update( entities: forUpdate, operations: operationUpdate ) }
            } )
            .store( in: &cancellables )
    }
    
    public override func Commit( keys: [Entity.ID], operation: REUpdateOperation = .update )
    {
        switch operation
        {
        case .delete:
            CommitDelete( keys: Set( keys ) )
            
        case .clear:
            CommitClear()
            
        default:
            Commit( repositoryKeys: keys.map { REEntityKey( $0 ) }, operation: operation )
        }
    }
    
    private func Commit( repositoryKeys keys: [REEntityKey], operation: REUpdateOperation = .update )
    {
        switch operation
        {
        case .delete:
            CommitDelete( keys: EntityKeys( repositoryKeys: Set( keys ) ) )
            
        case .clear:
            CommitClear()
            
        default:
            if let r = repository
            {
                r._RxGet( keys: keys )
                    .flatMap { self.RxRequestForCombine( source: "", entities: $0.map { Entity( entity: $0 ) }, updateChilds: false ).map { $0 }.eraseToAnyPublisher() }
                    .receive( on: queue )
                    .sink( receiveCompletion: { _ in }, receiveValue: { self.Commit( entities: $0, operation: operation ) } )
                    .store( in: &cancellables )
            }
            else
            {
                preconditionFailure( "To create Single with key you must specify singleFetchCallback or singleFetchBackCallback before" )
            }
        }
    }
    
    public override func Commit( keys: [Entity.ID], operations: [REUpdateOperation] )
    {
        let deleteKeys = Set( keys.enumerated().filter { operations[$0.0] == .delete }.map { $0.1 } )
        let otherKeys = keys.enumerated().filter { operations[$0.0] != .delete }.map { $0.1 }
        let otherOpers = operations.filter { $0 != .delete }
        
        if let _ = operations.first( where: { $0 == .clear } )
        {
            CommitClear()
        }
        
        CommitDelete( keys: deleteKeys )
        
        guard !otherKeys.isEmpty else { return }
        
        Commit( repositoryKeys: otherKeys.map { REEntityKey( $0 ) }, operations: otherOpers )
    }
    
    private func Commit( repositoryKeys keys: [REEntityKey], operations: [REUpdateOperation] )
    {
        let deleteKeys = Set( keys.enumerated().filter { operations[$0.0] == .delete }.map { $0.1 } )
        let otherKeys = keys.enumerated().filter { operations[$0.0] != .delete }.map { $0.1 }
        let otherOpers = operations.filter { $0 != .delete }
        
        if let _ = operations.first( where: { $0 == .clear } )
        {
            CommitClear()
        }
        
        CommitDelete( keys: EntityKeys( repositoryKeys: deleteKeys ) )
        
        guard !otherKeys.isEmpty else { return }
        
        if let r = repository
        {
            r._RxGet( keys: otherKeys )
                .flatMap { self.RxRequestForCombine( source: "", entities: $0.map { Entity( entity: $0 ) }, updateChilds: false ).map { $0 }.eraseToAnyPublisher() }
                .receive( on: queue )
                .sink( receiveCompletion: { _ in }, receiveValue: { self.Commit( entities: $0, operations: otherOpers ) } )
                .store( in: &cancellables )
        }
        else
        {
            preconditionFailure( "To create Single with key you must specify singleFetchCallback or singleFetchBackCallback before" )
        }
    }
    
    public override func Commit( keys: [Entity.ID], changes: (Entity) -> Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        var forUpdate = [Entity.ID: Entity]()
        keys.forEach
        {
            if let e = sharedEntities[$0]
            {
                let new = changes( e )
                sharedEntities[$0] = new
                forUpdate[$0] = new
            }
        }
        
        items.forEach { $0.ref?.Update( entities: forUpdate, operation: .update ) }
    }
    
    override func CommitDelete( keys: Set<Entity.ID> )
    {
        lock.lock()
        defer { lock.unlock() }
        
        keys.forEach { sharedEntities.removeValue( forKey: $0 ) }
        items.forEach { $0.ref?.Delete( keys: keys ) }
    }
    
    private func EntityKeys( repositoryKeys: Set<REEntityKey> ) -> Set<Entity.ID>
    {
        lock.lock()
        defer { lock.unlock() }
        
        return Set( sharedEntities.values.filter { repositoryKeys.contains( $0.reKey ) }.map { $0.id } )
    }
    
    override func CommitClear()
    {
        lock.lock()
        defer { lock.unlock() }
        
        sharedEntities = [:]
        items.forEach { $0.ref?.Clear() }
    }

    //MARK: - Updates
    public func RxRequestForUpdate( source: String = "", key: Entity.ID, update: @escaping (Entity) -> Entity ) -> RESingle<Entity?>
    {
        return REDeferred
        {
            [weak self] promise in
            
            if let entity = self?.sharedEntities[key]
            {
                let new = update( entity )
                self?.Update( source: source, entity: new )
                promise( .success( new ) )
            }
            else
            {
                promise( .success( nil ) )
            }
        }
    }
    
    public func RxRequestForUpdate( source: String = "", keys: [Entity.ID], update: @escaping (Entity) -> Entity ) -> RESingle<[Entity]>
    {
        return REDeferred
        {
            [weak self] promise in
            
            var updArr = [Entity](), updMap = [Entity.ID: Entity]()
            keys.forEach
            {
                if let entity = self?.sharedEntities[$0]
                {
                    let new = update( entity )
                    self?.sharedEntities[$0] = new
                    updArr.append( new )
                    updMap[$0] = new
                }
            }
            
            self?.items.forEach { $0.ref?.Update( source: source, entities: updMap ) }
            promise( .success( updArr ) )
        }
    }
    
    public func RxRequestForUpdate( source: String = "", update: @escaping (Entity) -> Entity ) -> RESingle<[Entity]>
    {
        return RxRequestForUpdate( source: source, keys: sharedEntities.keys.map { $0 }, update: update )
    }
    
    public func RxRequestForUpdate<EntityS: REEntity>( source: String = "", entities: [EntityS.ID: EntityS], underPathes: [KeyPath<Entity, any REEntity>], update: @escaping (Entity, EntityS) -> Entity ) -> RESingle<[Entity]>
    {
        return REDeferred
        {
            [weak self] promise in
            
            var updArr = [Entity](), updMap = [Entity.ID: Entity]()
            let Update: (Entity, EntityS) -> Void = {
                let new = update( $0, $1 )
                self?.sharedEntities[$0.id] = new
                updArr.append( new )
                updMap[$0.id] = new
            }
            self?.sharedEntities.forEach
            {
                e0 in
                
                underPathes.forEach
                {
                    if let v = e0.value[keyPath: $0] as? EntityS, let es = entities[v.id]
                    {
                        Update( e0.value, es )
                    }
                }
            }
            
            self?.items.forEach { $0.ref?.Update( source: source, entities: updMap ) }
            promise( .success( updArr ) )
        }
    }
    
    public func DispatchUpdates<EntityS: REEntity, TargetCollectionExtra>( to: REEntityObservableCollectionExtra<EntityS, TargetCollectionExtra>, withPathes: [KeyPath<EntityS, any REEntity>] )
    {
        
    }
    
    public func DispatchUpdates<V, TargetEntity: REEntity, TargetCollectionExtra>( to: REEntityObservableCollectionExtra<TargetEntity, TargetCollectionExtra>, fromPathes: [KeyPath<Entity, V>], apply: (V) -> TargetEntity )
    {
        
    }
    
    public func Refresh( resetCache: Bool = false, collectionExtra: CollectionExtra? = nil )
    {
        _Refresh( resetCache: resetCache, collectionExtra: collectionExtra )
    }
    
    func _Refresh( resetCache: Bool = false, collectionExtra: CollectionExtra? = nil )
    {
        lock.lock()
        defer { lock.unlock() }
        
        self.collectionExtra = collectionExtra ?? self.collectionExtra
        items.forEach { $0.ref?.RefreshData( resetCache: resetCache, data: self.collectionExtra ) }
    }
}

public typealias REEntityObservableCollection<Entity: REEntity> = REEntityObservableCollectionExtra<Entity, REEntityCollectionExtraParamsEmpty>

extension Publisher
{
    public func bind<Entity: REEntity>( refresh: REEntityObservableCollectionExtra<Entity, Output>, resetCache: Bool = false ) -> AnyCancellable
    {
        return receive( on: refresh.queue )
            .sink( receiveCompletion: { _ in }, receiveValue: { refresh._Refresh( resetCache: resetCache, collectionExtra: $0 ) } )
    }
}
