//
//  Array+Ext.swift
//  CombineEntity
//
//  Created by ALEXEY ABDULIN on 14.09.2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation

extension Array
{
    func asMapArray<K: Hashable>( by: KeyPath<Element, K> ) -> [K: [Element]]
    {
        var map = [K: [Element]]()
        
        forEach
        {
            let k = $0[keyPath: by]
            if map[k] == nil
            {
                map[k] = [$0]
            }
            else
            {
                map[k]!.append( $0 )
            }
        }
        
        return map
    }
}

extension Array where Element: REEntity
{
    public mutating func AppendOrReplace( entity: Element )
    {
        if let i = self.firstIndex( where: { entity._key == $0._key } )
        {
            self[i] = entity
        }
        else
        {
            self.append( entity )
        }
    }
    
    public mutating func AppendNotExist( entity: Element )
    {
        if let _ = self.firstIndex( where: { entity._key == $0._key } )
        {
            
        }
        else
        {
            self.append( entity )
        }
    }
    
    public mutating func AppendOrReplace( entities: [Element] )
    {
        entities.forEach
        {
            e in
            if let i = self.firstIndex( where: { e._key == $0._key } )
            {
                self[i] = e
            }
            else
            {
                self.append( e )
            }
        }
    }
    
    public mutating func Remove( entity: Element )
    {
        Remove( key: entity._key )
    }
    
    public mutating func Remove( key: REEntityKey )
    {
        if let i = self.firstIndex( where: { key == $0._key } )
        {
            remove( at: i )
        }
    }
    
    public func asEntitiesMap() -> [REEntityKey: Element]
    {
        var map = [REEntityKey: Element]()
        forEach { map[$0._key] = $0 }
        return map
    }
    
    public func Find( key: REEntityKey ) -> Element?
    {
        return first( where: { $0._key == key } )
    }
}

extension Array where Element == REEntityKey
{
    public mutating func AppendNotExist( key: Element )
    {
        if let _ = self.firstIndex( where: { key == $0 } )
        {
            
        }
        else
        {
            self.append( key )
        }
    }
    
    public mutating func Remove( key: Element )
    {
        if let i = self.firstIndex( where: { key == $0 } )
        {
            remove( at: i )
        }
    }
}
