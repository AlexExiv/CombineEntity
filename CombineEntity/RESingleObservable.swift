//
//  RESingleObservable.swift
//  CombineEntity
//
//  Created by ALEXEY ABDULIN on 22/01/2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation
import Combine

///Represents Observable that contains only one element
///- Parameters:
///- `Entity`: Type of entity
///- `Extra`: Any extra type which passes to the `fetch` closure for using during the data fetching
public class RESingleObservableExtra<Entity: REEntity, Extra>: REEntityObservable<Entity>, Publisher
{
    public typealias Output = Entity?
    public typealias Failure = Never
    public typealias Element = Entity?
    
    public enum State: Sendable
    {
        /// `initializing` - Entity is initializing no data has been loaded yet
        case initializing
        /// `ready` - Enity's data has been loaded
        case ready
        /// `notFound` - Entity not found
        case notFound
        /// `deleted` - Entity has been delete during its using
        case deleted
    }
    
    let queue: DispatchQueue
    
    /// Current state of the entity
    public let rxState = CurrentValueSubject<State, Never>( .initializing )
    let rxPublish = CurrentValueSubject<Entity?, Never>( nil )
    
    public private(set) var extra: Extra? = nil
    
    /// The key of the current entity
    public var key: REEntityKey? = nil
    
    /// The data of the current entity, nil if data hasn't been loaded yet or not found or record deleted
    public var entity: Entity?
    {
        return rxPublish.value
    }
    
    init( holder: REEntityCollection<Entity>, key: REEntityKey? = nil, extra: Extra? = nil, observeOn: DispatchQueue )
    {
        self.queue = observeOn
        self.key = key
        self.extra = extra
        
        super.init( holder: holder )
    }
    
    override func Update( source: String, entity: Entity )
    {
        if let key = key ?? self.entity?._key, key == entity._key, source != uuid
        {
            rxPublish.send( entity )
            rxState.send( .ready )
        }
    }
    
    override func Update( source: String, entities: [REEntityKey: Entity] )
    {
        if let key = key ?? entity?._key, let entity = entities[key], source != uuid
        {
            rxPublish.send( entity )
            rxState.send( .ready )
        }
    }
    
    override func Update( entities: [REEntityKey: Entity], operation: REUpdateOperation )
    {
        if let k = key ?? entity?._key, let e = entities[k]
        {
            switch operation
            {
            case .none, .insert, .update:
                rxPublish.send( e )
                rxState.send( .ready )
                
            case .delete, .clear:
                Clear()
            }
        }
    }
    
    override func Update( entities: [REEntityKey: Entity], operations: [REEntityKey: REUpdateOperation] )
    {
        if let k = key ?? entity?._key, let e = entities[k], let o = operations[k]
        {
            switch o
            {
            case .none, .insert, .update:
                rxPublish.send( e )
                rxState.send( .ready )
                
            case .delete, .clear:
                Clear()
            }
        }
    }
    
    override func Delete( keys: Set<REEntityKey> )
    {
        if let k = key ?? entity?._key, keys.contains( k )
        {
            Clear()
        }
    }
    
    override func Clear()
    {
        rxPublish.send( nil )
        rxState.send( .deleted )
    }
    
    func Set( key: REEntityKey )
    {
        self.key = key
    }
    
    /// Requests refreshing of the data
    /// - Parameters:
    ///   - resetCache: flag that's passed to the fetch block
    ///   - extra: optional extra data that's passed to the fetch block for filtering or any reason to get some extra information about the entity
    public func Refresh( resetCache: Bool = false, extra: Extra? = nil )
    {
        
    }
    
    func _Refresh( resetCache: Bool = false, extra: Extra? = nil )
    {
        lock.lock()
        defer { lock.unlock() }
        
        self.extra = extra ?? self.extra
    }

    //MARK: - Publisher
    public func receive<S>( subscriber: S ) where S: Subscriber, Never == S.Failure, Entity? == S.Input
    {
        rxPublish.receive( subscriber: subscriber )
    }
    
    public func asPublisher() -> AnyPublisher<Element, Never>
    {
        return rxPublish.eraseToAnyPublisher()
    }
}

public typealias RESingleObservable<Entity: REEntity> = RESingleObservableExtra<Entity, REEntityExtraParamsEmpty>

extension Publisher
{
    public func bind<Entity: REEntity>( refresh: RESingleObservableExtra<Entity, Output>, resetCache: Bool = false ) -> AnyCancellable
    {
        return receive( on: refresh.queue )
            .sink( receiveCompletion: { _ in }, receiveValue: { refresh._Refresh( resetCache: resetCache, extra: $0 ) } )
    }
}
