//
//  REEntity.swift
//  CombineEntity
//
//  Created by ALEXEY ABDULIN on 22/01/2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation

public typealias REEntityKey = AnyHashable

public protocol REEntity: Identifiable, Sendable where ID: Hashable & Sendable
{
    init( entity: any REBackEntityProtocol )
}

public extension REEntity
{
    var reKey: REEntityKey
    {
        REEntityKey( id )
    }
    
    init( entity: any REBackEntityProtocol )
    {
        preconditionFailure( "Default implimentation is prohibited to call" )
    }
}

public protocol REBackEntityProtocol: Identifiable, Sendable where ID: Hashable & Sendable
{
    init( entity: any REBackEntityProtocol )
}

public extension REBackEntityProtocol
{
    var reKey: REEntityKey
    {
        REEntityKey( id )
    }
}

public extension AnyHashable
{
    /// Returns string representation of the key
    var stringKey: String
    {
        base as! String
    }
    
    /// Returns int representation of the key
    var intKey: Int
    {
        base as! Int
    }
}
