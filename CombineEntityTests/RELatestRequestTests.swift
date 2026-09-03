//
//  RELatestRequestTests.swift
//  CombineEntityTests
//
//  Created by Codex on 03.09.2026.
//

import Foundation
import Combine
import Testing
@testable import CombineEntity

func DelayedEntitySingle( _ entity: TestEntity?, milliseconds: Int ) -> RESingle<TestEntity?>
{
    Just( entity )
        .delay( for: .milliseconds( milliseconds ), scheduler: DispatchQueue.global() )
        .setFailureType( to: Error.self )
        .eraseToAnyPublisher()
}

func DelayedEntityArray( _ entities: [TestEntity], milliseconds: Int ) -> RESingle<[TestEntity]>
{
    Just( entities )
        .delay( for: .milliseconds( milliseconds ), scheduler: DispatchQueue.global() )
        .setFailureType( to: Error.self )
        .eraseToAnyPublisher()
}

final class RELatestRequestCounter: @unchecked Sendable
{
    private let lock = NSLock()
    private var value = 0
    
    func Next() -> Int
    {
        lock.lock()
        defer { lock.unlock() }
        
        value += 1
        return value
    }
}

@Suite
struct RELatestRequestTests
{
    @Test func singleLatestRefreshWins() async throws
    {
        let collection = REEntityObservableCollection<TestEntity>( queue: DispatchQueue( label: "test.latest.single" ) )
        let counter = RELatestRequestCounter()
        let single = collection.CreateSingle( start: false )
        {
            _ in
            
            let index = counter.Next()
            let value = index == 1 ? "old" : "new"
            let delay = index == 1 ? 120 : 10
            return DelayedEntitySingle( TestEntity( id: "1", value: value ), milliseconds: delay )
        }
        
        single.Refresh()
        single.Refresh()
        
        let entity = try await WaitForSingle( single ) { $0.value == "new" }
        #expect( entity.value == "new" )
        
        try await Task.sleep( nanoseconds: 180_000_000 )
        #expect( single.entity?.value == "new" )
    }
    
    @Test func keyArrayLatestKeysWins() async throws
    {
        let collection = REEntityObservableCollection<TestEntity>( queue: DispatchQueue( label: "test.latest.keyarray" ) )
        let array = collection.CreateKeyArray( keys: ["1"] )
        {
            params in
            
            let key = params.keys.first ?? ""
            let value = key == "1" ? "old" : "new"
            let delay = key == "1" ? 120 : 10
            return DelayedEntityArray( [TestEntity( id: key, value: value )], milliseconds: delay )
        }
        
        array.keys = ["2"]
        
        let entities = try await WaitForArray( array ) { $0.first?.value == "new" }
        #expect( entities.first?.value == "new" )
        
        try await Task.sleep( nanoseconds: 180_000_000 )
        #expect( array.entities.first?.value == "new" )
    }
    
    @Test func paginatorLatestRefreshWins() async throws
    {
        let collection = REEntityObservableCollection<TestEntity>( queue: DispatchQueue( label: "test.latest.paginator" ) )
        let counter = RELatestRequestCounter()
        let paginator = collection.CreatePaginator( start: false )
        {
            _ in
            
            let index = counter.Next()
            let value = index == 1 ? "old" : "new"
            let delay = index == 1 ? 120 : 10
            return DelayedEntityArray( [TestEntity( id: "1", value: value )], milliseconds: delay )
        }
        
        paginator.Refresh()
        paginator.Refresh()
        
        let entities = try await WaitForArray( paginator ) { $0.first?.value == "new" }
        #expect( entities.first?.value == "new" )
        
        try await Task.sleep( nanoseconds: 180_000_000 )
        #expect( paginator.entities.first?.value == "new" )
    }
    
    @Test func suspendResumeKeepsLatestRequest() async throws
    {
        let collection = REEntityObservableCollection<TestEntity>( queue: DispatchQueue( label: "test.latest.resume" ) )
        let single = collection.CreateSingleExtra( extra: Optional<String>.none, start: false )
        {
            params in
            
            let value = params.extra == "old" ? "old" : "new"
            return DelayedEntitySingle( TestEntity( id: "1", value: value ), milliseconds: 10 )
        }
        
        single.Suspend()
        single.Refresh( extra: "old" )
        single.Refresh( extra: "new" )
        single.Resume()
        
        let entity = try await WaitForSingle( single ) { $0.value == "new" }
        #expect( entity.value == "new" )
    }
}
