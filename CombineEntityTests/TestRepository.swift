//
//  TestRepository.swift
//  CombineEntityTests
//
//  Created by ALEXEY ABDULIN on 13.10.2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation
import Combine
@testable import CombineEntity

protocol TestEntityBackProtocol: REBackEntityProtocol
{
    var id: String { get }
    var value: String { get }
    var indirectId: String { get }
    var indirectValue: String { get }
    
    init( entity: any TestEntityBackProtocol )
}

extension TestEntityBackProtocol
{
    var _key: REEntityKey { return REEntityKey( id ) }
    
    init( entity: any REBackEntityProtocol )
    {
        self.init( entity: entity as! any TestEntityBackProtocol )
    }
}

struct TestEntityBack: TestEntityBackProtocol
{
    let id: String
    let value: String
    let indirectId: String
    let indirectValue: String
    
    init( id: String, value: String, indirectId: String = "", indirectValue: String = "" )
    {
        self.id = id
        self.value = value
        self.indirectId = indirectId
        self.indirectValue = indirectValue
    }
    
    init( entity: any TestEntityBackProtocol )
    {
        id = entity.id
        value = entity.value
        indirectId = entity.indirectId
        indirectValue = entity.indirectValue
    }
}

protocol IndirectEntityBackProtocol: REBackEntityProtocol
{
    var id: String { get }
    var value: String { get }
    
    init( entity: any TestEntityBackProtocol )
}

extension IndirectEntityBackProtocol
{
    var _key: REEntityKey { return REEntityKey( id ) }
    
    init( entity: any REBackEntityProtocol )
    {
        self.init( entity: entity as! any TestEntityBackProtocol )
    }
}

struct IndirectEntityBack: IndirectEntityBackProtocol
{
    let id: String
    let value: String
    
    init( id: String, value: String )
    {
        self.id = id
        self.value = value
    }
    
    init( entity: any TestEntityBackProtocol )
    {
        id = entity.id
        value = entity.value
    }
}

final class TestRepository<Entity: REBackEntityProtocol>: REEntityRepository<Entity>
{
    var items: [Entity] = []

    func Add( entities: [Entity] )
    {
        items.append( contentsOf: entities )
        rxEntitiesUpdated.send( entities.map { REEntityUpdated( key: $0._key, operation: .insert ) } )
    }
    
    func Update( entity: Entity )
    {
        if let i = items.firstIndex( where: { entity._key == $0._key } )
        {
            items[i] = entity
        }
        else
        {
            items.append( entity )
        }
        rxEntitiesUpdated.send( [REEntityUpdated( key: entity._key, operation: .update )] )
    }
    
    func Delete( key: REEntityKey )
    {
        items.removeAll( where: { $0._key == key } )
        rxEntitiesUpdated.send( [REEntityUpdated( key: key, operation: .delete )] )
    }
    
    func Clear()
    {
        items.removeAll()
        rxEntitiesUpdated.send( [REEntityUpdated( key: 0, operation: .clear )] )
    }
    
    override func RxGet( key: REEntityKey ) -> RESingle<Entity?>
    {
        return REJust( items.first(where: { $0._key == key } ) )
    }
    
    override func RxGet( keys: [REEntityKey] ) -> RESingle<[Entity]>
    {
        return REJust( items.filter { keys.contains( $0._key ) } )
    }
}

typealias TestRepositoryIndirect = TestRepository<IndirectEntityBack>

final class TestRepositoryDirect: REEntityRepository<TestEntityBack>
{
    var items: [TestEntityBack] = []
    let second: TestRepositoryIndirect
    
    init( second: TestRepositoryIndirect )
    {
        self.second = second
        super.init()
    }
    
    func Add( entities: [TestEntityBack] )
    {
        items.append( contentsOf: entities )
        rxEntitiesUpdated.send( entities.map { REEntityUpdated( key: $0._key, operation: .insert ) } )
    }
    
    override func RxGet( key: REEntityKey ) -> RESingle<TestEntityBack?>
    {
        return REJust( items.first(where: { $0._key == key } ) )
            .flatMap { $0 == nil ? REJust( nil ) : self.RxLoad( entities: [$0!] ).map { $0.first }.eraseToAnyPublisher() }
            .eraseToAnyPublisher()
    }
    
    override func RxGet( keys: [REEntityKey] ) -> RESingle<[TestEntityBack]>
    {
        return REJust( items.filter { keys.contains( $0._key ) } )
            .flatMap { self.RxLoad( entities: $0 ) }
            .eraseToAnyPublisher()
    }
    
    func RxLoad( entities: [TestEntityBack] ) -> RESingle<[TestEntityBack]>
    {
        let keys = entities.map { REEntityKey( $0.indirectId ) }
        return second
            .RxGet( keys: keys )
            .map { $0.asEntitiesMap() }
            .map { m in entities.map { TestEntityBack( id: $0.id, value: $0.value, indirectId: $0.indirectId, indirectValue: m[REEntityKey( $0.indirectId )]?.value ?? "" ) } }
            .eraseToAnyPublisher()
    }
}

extension Array where Element: REBackEntityProtocol
{
    public func asEntitiesMap() -> [REEntityKey: Element]
    {
        var map = [REEntityKey: Element]()
        forEach { map[$0._key] = $0 }
        return map
    }
}
