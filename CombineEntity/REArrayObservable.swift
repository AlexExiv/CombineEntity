//
//  REArrayObservable.swift
//  CombineEntity
//
//  Created by ALEXEY ABDULIN on 29/01/2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation
import Combine

public enum REArrayUpdatePolicy: Sendable
{
    case update, reload
}

public let RE_ARRAY_PER_PAGE = 999999

///Represents Observable that contains limited number of element. For example the list of cities or stores in the city
///- Parameters:
///- `Entity`: Type of entity
///- `Extra`: Any extra type which passes to the `fetch` closure for using during the data fetching
public class REArrayObservableExtra<Entity: REEntity, Extra>: REEntityObservable<Entity>, Publisher
{
    public typealias Output = [Entity]
    public typealias Failure = Never
    public typealias Element = [Entity]
    
    let rxPublish = CurrentValueSubject<Element, Never>( [] )
    let queue: DispatchQueue

    public private(set) var page = -1
    public private(set) var perPage = RE_ARRAY_PER_PAGE
    public private(set) var extra: Extra? = nil

    /// Elements of observale
    public private(set) var entities: [Entity] = []
    
    public var updatePolicy: REArrayUpdatePolicy = .update
   
    init( holder: REEntityCollection<Entity>, extra: Extra? = nil, perPage: Int = RE_ARRAY_PER_PAGE, start: Bool = true, observeOn: DispatchQueue )
    {
        self.queue = observeOn
        self.extra = extra
        self.perPage = perPage
        
        super.init( holder: holder )
    }
    
    //MARK: - Update
    override func Update( source: String, entity: Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        if let i = entities.firstIndex( where: { entity._key == $0._key } ), source != uuid
        {
            entities[i] = entity
            rxPublish.send( entities )
        }
    }
    
    override func Update( source: String, entities: [REEntityKey: Entity] )
    {
        guard source != uuid else { return }
        
        lock.lock()
        defer { lock.unlock() }
        
        var was = false
        for i in 0..<self.entities.count
        {
            let e = self.entities[i]
            if let ne = entities[e._key]
            {
                self.entities[i] = ne
                was = true
            }
        }
        
        if was
        {
            rxPublish.send( self.entities )
        }
    }
    
    override func Update( entities: [REEntityKey: Entity], operation: REUpdateOperation )
    {
        lock.lock()
        defer { lock.unlock() }

        switch operation
        {
        case .insert,
             .update where updatePolicy == .reload:
            Refresh( extra: extra )
            
        case .clear:
            Clear()
            
        default:
            let _entities = self.entities
            _entities.forEach
            {
                if let e = entities[$0._key]
                {
                    switch operation
                    {
                    case .update:
                        Set( entity: e )
                        
                    case .delete:
                        Remove( key: e._key )
                        
                    default:
                        break
                    }
                }
            }
        }
    }
    
    override func Update( entities: [REEntityKey: Entity], operations: [REEntityKey: REUpdateOperation] )
    {
        lock.lock()
        defer { lock.unlock() }
        
        if operations.values.contains( .insert ) || (updatePolicy == .reload && operations.values.contains( .update ))
        {
            Refresh( extra: extra )
        }
        else
        {
            let _entities = self.entities
            _entities.forEach
            {
                if let e = entities[$0._key], let o = operations[$0._key]
                {
                    switch o
                    {
                    case .update:
                        Set( entity: e )
                        
                    case .delete:
                        Remove( key: e._key )
                        
                    default:
                        break
                    }
                }
            }
        }
    }
    
    override func Delete( keys: Set<REEntityKey> )
    {
        let _entities = self.entities
        _entities.forEach
        {
            if keys.contains( $0._key )
            {
                Remove( key: $0._key )
            }
        }
    }
    
    override func Clear()
    {
        Set( entities: [] )
    }
    
    //MARK: - Set
    func Set( entity: Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        if let i = entities.firstIndex( where: { $0._key == entity._key } )
        {
            entities[i] = entity
            rxPublish.send( entities )
        }
    }
    
    func Set( entities: [Entity] )
    {
        lock.lock()
        defer { lock.unlock() }
        
        self.entities = entities
        rxPublish.send( self.entities )
    }
    
    public func Refresh( resetCache: Bool = false, extra: Extra? = nil )
    {
        
    }
    
    func _Refresh( resetCache: Bool = false, extra: Extra? = nil )
    {
        lock.lock()
        defer { lock.unlock() }
        
        self.extra = extra ?? self.extra
        page = -1
        if perPage != RE_ARRAY_PER_PAGE
        {
            Set( entities: [] )
        }
    }

    func Append( entities: [Entity] ) -> [Entity]
    {
        lock.lock()
        defer { lock.unlock() }
        
        page = PAGINATOR_END
        return entities
    }
    
    func Set( page: Int )
    {
        lock.lock()
        defer { lock.unlock() }
        
        self.page = page
    }
    
    //MARK: - Array operations
    /// Creates `SingleObservable` and sets the element from array as its value
    /// - Parameter index: index of  the element in the array
    public subscript( index: Int ) -> RESingleObservable<Entity>
    {
        lock.lock()
        defer { lock.unlock() }
        
        return collection!.CreateSingle( initial: entities[index] )
    }
    
    /// Add new element to the array. If element exists it changes by this
    /// - Parameter entity: entity for adding
    public func Append( entity: Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        entities.AppendNotExist( entity: entity )
        rxPublish.send( entities )
    }
    
    /// Remove element from the array
    /// - Parameter entity: entity for removing
    public func Remove( entity: Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        entities.Remove( entity: entity )
        rxPublish.send( entities )
    }
    
    /// Remove element from the array by its key
    /// - Parameter key: the key of the entity for removing
    public func Remove( key: REEntityKey )
    {
        lock.lock()
        defer { lock.unlock() }
        
        entities.Remove( key: key )
        rxPublish.send( entities )
    }
    
    //MARK: - Publisher
    public func receive<S>( subscriber: S ) where S: Subscriber, Never == S.Failure, [Entity] == S.Input
    {
        rxPublish.receive( subscriber: subscriber )
    }
    
    public func asPublisher() -> AnyPublisher<Element, Never>
    {
        return rxPublish.eraseToAnyPublisher()
    }
}

public typealias REArrayObservable<Entity: REEntity> = REArrayObservableExtra<Entity, REEntityExtraParamsEmpty>

extension Publisher
{
    public func bind<Entity: REEntity>( refresh: REArrayObservableExtra<Entity, Output>, resetCache: Bool = false ) -> AnyCancellable
    {
        return receive( on: refresh.queue )
            .sink( receiveCompletion: { _ in }, receiveValue: { refresh._Refresh( resetCache: resetCache, extra: $0 ) } )
    }
}
