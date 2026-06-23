//
//  REKeyArrayObservable.swift
//  CombineEntity
//
//  Created by ALEXEY ABDULIN on 07.10.2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation
import Combine

///Represents Observable that contains array of elements by its keys
///- Parameters:
///- `Entity`: Type of entity
///- `Extra`: Any extra type which passes to the `fetch` closure for using during the data fetching
public class REKeyArrayObservableExtra<Entity: REEntity, Extra>: REArrayObservableExtra<Entity, Extra>
{
    public var keys: [Entity.ID]
    {
        set
        {
            lock.lock()
            defer { lock.unlock() }
            
            innerKeys = newValue
        }
        get { innerKeys }
        
    }
    var innerKeys: [Entity.ID] = []

    init( holder: REEntityCollection<Entity>, keys: [Entity.ID] = [], extra: Extra? = nil, observeOn: DispatchQueue )
    {
        self.innerKeys = keys
        super.init( holder: holder, extra: extra, observeOn: observeOn )
    }
    
    override func Update( entities: [Entity.ID: Entity], operation: REUpdateOperation )
    {
        lock.lock()
        defer { lock.unlock() }
        
        let _entities = self.entities
        _entities.forEach {
            if let e = entities[$0.id]
            {
                Apply( entity: e, operation: operation )
            }
        }
    }
    
    override func Update( entities: [Entity.ID: Entity], operations: [Entity.ID: REUpdateOperation] )
    {
        lock.lock()
        defer { lock.unlock() }
        
        let _entities = self.entities
        _entities.forEach {
            if let e = entities[$0.id], let o = operations[$0.id]
            {
                Apply( entity: e, operation: o )
            }
        }
    }
    
    private func Apply( entity: Entity, operation: REUpdateOperation )
    {
        switch operation
        {
        case .none, .insert, .update:
            Set( entity: entity )
            
        case .delete:
            Remove( key: entity.id )
            
        case .clear:
            Clear()
        }
    }
    
    /// Add new key to the sequence of keys if the key exists nothing happens
    /// - Parameter key: key for adding
    public func Append( key: Entity.ID )
    {
        lock.lock()
        defer { lock.unlock() }
        
        innerKeys.AppendNotExist( key: key )
    }
    
    public override func Append( entity: Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        super.Append( entity: entity )
        innerKeys.AppendNotExist( key: entity.id )
    }
    
    public override func Remove( entity: Entity )
    {
        lock.lock()
        defer { lock.unlock() }
        
        super.Remove( entity: entity )
        innerKeys.Remove( key: entity.id )
    }

    public override func Remove( key: Entity.ID )
    {
        lock.lock()
        defer { lock.unlock() }
        
        super.Remove( key: key )
        innerKeys.Remove( key: key )
    }
    
    override func Clear()
    {
        keys = []
        Set( entities: [] )
    }
}

public typealias REKeyArrayObservable<Entity: REEntity> = REKeyArrayObservableExtra<Entity, REEntityExtraParamsEmpty>

extension Publisher
{
    public func bind<Entity: REEntity>( refresh: REKeyArrayObservableExtra<Entity, Output>, resetCache: Bool = false ) -> AnyCancellable
    {
        return receive( on: refresh.queue )
            .sink( receiveCompletion: { _ in }, receiveValue: { refresh._Refresh( resetCache: resetCache, extra: $0 ) } )
    }
}

extension Publisher
{
    public func bind<Entity: REEntity, Extra>( keys: REKeyArrayObservableExtra<Entity, Extra> ) -> AnyCancellable where Output == [Entity.ID]
    {
        return receive( on: keys.queue )
            .sink( receiveCompletion: { _ in }, receiveValue: { keys.keys = $0 } )
    }
}
