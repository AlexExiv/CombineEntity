//
//  REEntityCollection.swift
//  CombineEntity
//
//  Created by ALEXEY ABDULIN on 10/02/2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation
import Combine

struct REWeakObjectObservable<Entity: REEntity>
{
    weak var ref: REEntityObservable<Entity>?
}

public class REEntityCollection<Entity: REEntity>
{
    var items = [REWeakObjectObservable<Entity>]()
    var sharedEntities = [REEntityKey: Entity]()
        
    let lock = NSRecursiveLock()
    let queue: DispatchQueue
    var cancellables = Set<AnyCancellable>()
    
    init( queue: DispatchQueue )
    {
        self.queue = queue
    }
    
    func Add( object: REEntityObservable<Entity> )
    {
        lock.lock()
        defer { lock.unlock() }
        
        items.append( REWeakObjectObservable( ref: object ) )
    }
    
    func Remove( object: REEntityObservable<Entity> )
    {
        lock.lock()
        defer { lock.unlock() }
        
        items.removeAll( where: { object.uuid == $0.ref?.uuid } )
    }
    
    func Store( _ cancellable: AnyCancellable )
    {
        lock.lock()
        defer { lock.unlock() }
        
        cancellables.insert( cancellable )
    }
    
    func RxRequestForCombine( source: String = "", entity: Entity, updateChilds: Bool = true ) -> RESingle<Entity>
    {
        preconditionFailure( "" )
    }
    
    func RxRequestForCombine( source: String = "", entities: [Entity], updateChilds: Bool = true ) -> RESingle<[Entity]>
    {
        preconditionFailure( "" )
    }
    
    public func RxUpdate( source: String = "", entity: Entity ) -> RESingle<Entity>
    {
        return REDeferred
        {
            [weak self] promise in
            
            self?.Update( source: source, entity: entity )
            promise( .success( entity ) )
        }
    }
    
    public func RxUpdate( source: String = "", entities: [Entity] ) -> RESingle<[Entity]>
    {
        return REDeferred
        {
            [weak self] promise in
            
            self?.Update( source: source, entities: entities )
            promise( .success( entities ) )
        }
    }
    
    open func Update( source: String = "", entity: Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        sharedEntities[entity._key] = entity
        items.forEach { $0.ref?.Update( source: source, entity: entity ) }
    }
    
    open func Update( source: String = "", entities: [Entity] )
    {
        lock.lock()
        defer { lock.unlock() }
        
        entities.forEach { sharedEntities[$0._key] = $0 }
        items.forEach { $0.ref?.Update( source: source, entities: entities.asEntitiesMap() ) }
    }
    
    //MARK: - Commit
    public func Commit( entity: Entity, operation: REUpdateOperation = .update )
    {
        fatalError( "This method must be overridden" )
    }
    
    public func Commit( entity: any REBackEntityProtocol, operation: REUpdateOperation = .update )
    {
        Commit( entity: Entity( entity: entity ), operation: operation )
    }
    
    public func Commit( key: REEntityKey, operation: REUpdateOperation = .update )
    {
        fatalError( "This method must be overridden" )
    }
    
    public func Commit( key: REEntityKey, changes: (Entity) -> Entity )
    {
        fatalError( "This method must be overridden" )
    }
    
    public func Commit( entities: [Entity], operation: REUpdateOperation = .update )
    {
        fatalError( "This method must be overridden" )
    }
    
    public func Commit( entities: [any REBackEntityProtocol], operation: REUpdateOperation = .update )
    {
        Commit( entities: entities.map { Entity( entity: $0 ) }, operation: operation )
    }
    
    public func Commit( entities: [Entity], operations: [REUpdateOperation] )
    {
        fatalError( "This method must be overridden" )
    }
    
    public func Commit( entities: [any REBackEntityProtocol], operations: [REUpdateOperation] )
    {
        Commit( entities: entities.map { Entity( entity: $0 ) }, operations: operations )
    }
    
    public func Commit( keys: [REEntityKey], operation: REUpdateOperation = .update )
    {
        fatalError( "This method must be overridden" )
    }
    
    public func Commit( keys: [REEntityKey], operations: [REUpdateOperation] )
    {
        fatalError( "This method must be overridden" )
    }
    
    public func Commit( keys: [REEntityKey], changes: (Entity) -> Entity )
    {
        fatalError( "This method must be overridden" )
    }
    
    func CommitDelete( keys: Set<REEntityKey> )
    {
        fatalError( "This method must be overridden" )
    }
    
    func CommitClear()
    {
        fatalError( "This method must be overridden" )
    }
    
    //MARK: - Create
    func CreateSingle( initial: Entity, refresh: Bool = false ) -> RESingleObservable<Entity>
    {
        fatalError( "This method must be overridden" )
    }
    
    func CreateKeyArray( initial: [Entity] ) -> REKeyArrayObservable<Entity>
    {
        fatalError( "This method must be overridden" )
    }
    
    /// Get cached entity by its id
    /// - Parameter elementId: key of the entity
    /// - Returns: cached entity if it's exist or nil if the entity hasn't been cached yet
    public subscript ( entityKey id: REEntityKey ) -> Entity?
    {
        lock.lock()
        defer { lock.unlock() }
        
        return sharedEntities[id]
    }
}
