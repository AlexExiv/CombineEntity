//
//  REEntityObservableTests.swift
//  CombineEntityTests
//
//  Created by ALEXEY ABDULIN on 22/01/2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation
import Combine
import Testing
@testable import CombineEntity

struct TestEntity: REEntity, Equatable
{
    var _key: REEntityKey { return REEntityKey( id ) }
    
    let id: String
    let value: String
    let indirectId: String
    let indirectValue: String
    
    init( entity: any REBackEntityProtocol )
    {
        self.init( entity: entity as! any TestEntityBackProtocol )
    }
    
    init( entity: any TestEntityBackProtocol )
    {
        id = entity.id
        value = entity.value
        indirectId = entity.indirectId
        indirectValue = entity.indirectValue
    }
    
    init( id: String, value: String, indirectId: String = "", indirectValue: String = "" )
    {
        self.id = id
        self.value = value
        self.indirectId = indirectId
        self.indirectValue = indirectValue
    }
    
    func Modified( value: String ) -> TestEntity
    {
        return TestEntity( id: id, value: value, indirectId: indirectId, indirectValue: indirectValue )
    }
}

struct ExtraParams: Sendable
{
    let test: String
}

struct ExtraCollectionParams: Sendable
{
    let test: String
}

func BackSingle( _ entity: TestEntityBack? ) -> RESingle<(any REBackEntityProtocol)?>
{
    REJust( entity.map { $0 as any REBackEntityProtocol } )
}

func BackArray( _ entities: [TestEntityBack] ) -> RESingle<[any REBackEntityProtocol]>
{
    REJust( entities.map { $0 as any REBackEntityProtocol } )
}

func EntitySingle( _ entity: TestEntity? ) -> RESingle<TestEntity?>
{
    REJust( entity )
}

func DelayedEntitySingle( _ entity: TestEntity? ) -> RESingle<TestEntity?>
{
    Just( entity )
        .delay( for: .milliseconds( 20 ), scheduler: DispatchQueue.global() )
        .setFailureType( to: Error.self )
        .eraseToAnyPublisher()
}

func EntityArray( _ entities: [TestEntity] ) -> RESingle<[TestEntity]>
{
    REJust( entities )
}

@Suite
struct REEntityObservableTests
{
    @Test func loadingSingleArrayPaginatorAndUpdates() async throws
    {
        let collection = REEntityObservableCollection<TestEntity>( queue: DispatchQueue( label: "test.loading" ) )
        let single = collection.CreateSingleBack( key: "1" ) { _ in BackSingle( TestEntityBack( id: "1", value: "2" ) ) }
        
        var s = try await WaitForSingle( single ) { $0.value == "2" }
        #expect( s.id == "1" )
        #expect( s.value == "2" )
        
        let pages = collection.CreatePaginatorBack { _ in BackArray( [TestEntityBack( id: "1", value: "3" ), TestEntityBack( id: "2", value: "4" )] ) }
        var a = try await WaitForArray( pages ) { $0.count == 2 && $0[0].value == "3" }
        #expect( pages.page == PAGINATOR_END )
        #expect( a[0].id == "1" )
        #expect( a[1].id == "2" )
        
        s = try await WaitForSingle( single ) { $0.value == "3" }
        #expect( s.value == "3" )
        
        _ = try await WaitSingle( collection.RxRequestForUpdate( key: "1" ) { $0.Modified( value: "10" ) } )
        s = try await WaitForSingle( single ) { $0.value == "10" }
        a = try await WaitForArray( pages ) { $0.first?.value == "10" }
        #expect( s.value == "10" )
        #expect( a[0].value == "10" )
        
        _ = try await WaitSingle( collection.RxRequestForUpdate( keys: ["1", "2"] ) { $0.Modified( value: "1\($0.id)" ) } )
        s = try await WaitForSingle( single ) { $0.value == "11" }
        a = try await WaitForArray( pages )
        {
            (items: [TestEntity]) in
            
            items.count == 2 && items[0].value == "11" && items[1].value == "12"
        }
        #expect( s.value == "11" )
        #expect( a[1].value == "12" )
        
        let updated = try await WaitSingle( collection.RxUpdate( entity: TestEntity( id: "1", value: "25" ) ) )
        s = try await WaitForSingle( single ) { $0.value == "25" }
        #expect( updated.value == "25" )
        #expect( s.value == "25" )
        
        single.Refresh()
        s = try await WaitForSingle( single ) { $0.value == "2" }
        #expect( s.value == "2" )
        
        pages.Refresh()
        s = try await WaitForSingle( single ) { $0.value == "3" }
        #expect( s.value == "3" )
    }
    
    @Test func extraParamsRefresh() async throws
    {
        let collection = REEntityObservableCollection<TestEntity>( queue: DispatchQueue( label: "test.extra" ) )
        let single = collection.CreateSingleExtra( extra: ExtraParams( test: "test" ) )
        {
            params in
            
            if params.first
            {
                #expect( params.extra?.test == "test" )
            }
            else
            {
                #expect( params.extra?.test == "test2" )
                #expect( params.refreshing )
            }
            
            return EntitySingle( TestEntity( id: "1", value: "2" ) )
        }
        
        _ = try await WaitForSingle( single ) { $0.value == "2" }
        single.Refresh( extra: ExtraParams( test: "test2" ) )
        _ = try await WaitForSingle( single ) { $0.value == "2" }
        
        let pages = collection.CreatePaginatorExtra( extra: ExtraParams( test: "test" ) )
        {
            params in
            
            if params.first
            {
                #expect( params.extra?.test == "test" )
            }
            else
            {
                #expect( params.extra?.test == "test2" )
                #expect( params.refreshing )
                #expect( params.page == 0 )
            }
            
            return EntityArray( [TestEntity( id: "1", value: "3" ), TestEntity( id: "2", value: "4" )] )
        }
        
        _ = try await WaitForArray( pages ) { $0.count == 2 && $0[0].value == "3" }
        pages.Refresh( extra: ExtraParams( test: "test2" ) )
        _ = try await WaitForArray( pages ) { $0.count == 2 && $0[0].value == "3" }
        
        let item = collection[entityKey: "1"]
        #expect( item?.id == "1" )
        #expect( item?.value == "3" )
    }
    
    @Test func collectionExtraRefresh() async throws
    {
        let collection = REEntityObservableCollectionExtra<TestEntity, ExtraCollectionParams>( queue: DispatchQueue( label: "test.collection.extra" ), collectionExtra: ExtraCollectionParams( test: "test" ) )
        let single = collection.CreateSingleBackExtra( extra: ExtraParams( test: "test" ) )
        {
            params in
            
            if params.first
            {
                #expect( params.collectionExtra?.test == "test" )
            }
            else
            {
                #expect( params.extra?.test == "test" )
                #expect( params.collectionExtra?.test == "test2" )
                #expect( params.refreshing )
            }
            
            return BackSingle( TestEntityBack( id: "single", value: params.collectionExtra?.test ?? "" ) )
        }
        
        _ = try await WaitForSingle( single ) { $0.value == "test" }

        let pages = collection.CreatePaginatorBackExtra( extra: ExtraParams( test: "test" ) )
        {
            params in
            
            if params.first
            {
                #expect( params.collectionExtra?.test == "test" )
            }
            else
            {
                #expect( params.extra?.test == "test" )
                #expect( params.collectionExtra?.test == "test2" )
                #expect( params.refreshing )
                #expect( params.page == 0 )
            }
            
            return BackArray( [TestEntityBack( id: "1", value: params.collectionExtra!.test + "1" ), TestEntityBack( id: "2", value: params.collectionExtra!.test + "2" )] )
        }
        
        _ = try await WaitForArray( pages ) { $0.count == 2 && $0[0].value == "test1" }
        collection.Refresh( collectionExtra: ExtraCollectionParams( test: "test2" ) )

        _ = try await WaitForSingle( single ) { $0.value == "test2" }
        _ = try await WaitForArray( pages ) { $0.count == 2 && $0[0].value == "test21" }
    }
    
    @Test func arrayGetSingleAndCollectionRefresh() async throws
    {
        var i = 0
        let collection = REEntityObservableCollectionExtra<TestEntity, ExtraCollectionParams>( queue: DispatchQueue( label: "test.array.single" ), collectionExtra: ExtraCollectionParams( test: "test" ) )
        collection.singleFetchBackCallback =
        {
            params in
            
            BackSingle( TestEntityBack( id: params.lastEntity!.id, value: params.collectionExtra!.test + (i == 0 ? "sr" : "") + params.lastEntity!.id ) )
        }
        
        let pages = collection.CreatePaginatorBackExtra( extra: ExtraParams( test: "test" ) )
        {
            params in
            
            BackArray( [TestEntityBack( id: "1", value: params.collectionExtra!.test + "1" ), TestEntityBack( id: "2", value: params.collectionExtra!.test + "2" )] )
        }
        
        #expect( pages.perPage != RE_ARRAY_PER_PAGE )
        _ = try await WaitForArray( pages ) { $0.count == 2 }
        
        let single0 = pages[0]
        let single1 = pages[1]
        
        var s0 = try await WaitForSingle( single0 ) { $0.value == "test1" }
        var s1 = try await WaitForSingle( single1 ) { $0.value == "test2" }
        #expect( s0.id == "1" )
        #expect( s1.id == "2" )
        
        single0.Refresh()
        single1.Refresh()
        
        s0 = try await WaitForSingle( single0 ) { $0.value == "testsr1" }
        s1 = try await WaitForSingle( single1 ) { $0.value == "testsr2" }
        #expect( s0.value == "testsr1" )
        #expect( s1.value == "testsr2" )
        
        i = 1
        collection.Refresh( collectionExtra: ExtraCollectionParams( test: "test2" ) )

        s0 = try await WaitForSingle( single0 ) { $0.value == "test21" }
        s1 = try await WaitForSingle( single1 ) { $0.value == "test22" }
        #expect( s0.value == "test21" )
        #expect( s1.value == "test22" )
    }
    
    @Test func arrayInitialAndKeys() async throws
    {
        let collection = REEntityObservableCollectionExtra<TestEntity, ExtraCollectionParams>( queue: DispatchQueue( label: "test.array.initial" ), collectionExtra: ExtraCollectionParams( test: "test" ) )
        collection.arrayFetchBackCallback =
        {
            params in
            
            BackArray( params.keys.map { TestEntityBack( id: $0.stringKey, value: params.collectionExtra!.test + $0.stringKey ) } )
        }
        
        let array = collection.CreateKeyArray( initial: [TestEntity( id: "1", value: "3" ), TestEntity( id: "2", value: "4" )] )
        #expect( array.perPage == RE_ARRAY_PER_PAGE )
        
        var s = try await WaitForArray( array ) { $0.count == 2 && $0[0].value == "3" }
        #expect( s[0].value == "3" )
        #expect( s[1].value == "4" )
        
        collection.Refresh()
        s = try await WaitForArray( array ) { $0.count == 2 && $0[0].value == "test1" }
        #expect( s[0].value == "test1" )
        #expect( s[1].value == "test2" )
        
        collection.Refresh( collectionExtra: ExtraCollectionParams( test: "test2" ) )
        s = try await WaitForArray( array ) { $0.count == 2 && $0[0].value == "test21" }
        #expect( s[0].value == "test21" )
        #expect( s[1].value == "test22" )
        
        array.Append( key: "3" )
        s = try await WaitForArray( array ) { $0.count == 3 && $0[2].value == "test23" }
        #expect( s[2].id == "3" )
        
        array.Append( entity: TestEntity( id: "4", value: "Appended" ) )
        s = try await WaitForArray( array ) { $0.count == 4 && $0[3].value == "Appended" }
        #expect( s[3].value == "Appended" )
        
        array.keys = ["3", "4"]
        s = try await WaitForArray( array )
        {
            (items: [TestEntity]) in
            
            items.count == 2 && items[0].id == "3" && items[1].id == "4"
        }
        #expect( s[0].value == "test23" )
        #expect( s[1].value == "Appended" )
    }
    
    @Test func combineLatestSingleDeclineAndSixSources() async throws
    {
        let collection = REEntityObservableCollectionExtra<TestEntity, ExtraCollectionParams>( queue: DispatchQueue( label: "test.combine.single" ) )
        let rxObs = CurrentValueSubject<String, Never>( "2" )
        let rxObs1 = CurrentValueSubject<String, Never>( "3" )
        collection.combineLatest( rxObs, rxObs1, apply: { $0.Modified( value: $1 + $2 ) } )
        collection.combineLatest( rxObs, apply: { $0.Modified( value: $1 ) } )
        collection.combineLatest( rxObs1, apply: { $0.Modified( value: $1 ) } )
        
        let single0 = collection.CreateSingleBack( key: "1" ) { _ in BackSingle( TestEntityBack( id: "1", value: "1" ) ) }
        var s = try await WaitForSingle( single0 ) { $0.value == "3" }
        #expect( s.value == "3" )

        rxObs.send( "4" )
        rxObs1.send( "4" )
        s = try await WaitForSingle( single0 ) { $0.value == "4" }
        #expect( s.value == "4" )

        rxObs1.send( "5" )
        s = try await WaitForSingle( single0 ) { $0.value == "5" }
        #expect( s.value == "5" )
        
        let declineCollection = REEntityObservableCollectionExtra<TestEntity, ExtraCollectionParams>( queue: DispatchQueue( label: "test.combine.decline" ) )
        let decline = CurrentValueSubject<String, Never>( "2" )
        declineCollection.combineLatest( decline, test: { $0.1 != "4" }, apply: { $0.0.Modified( value: $0.1 ) } )
        let declineSingle = declineCollection.CreateSingleBack( key: "1" ) { _ in BackSingle( TestEntityBack( id: "1", value: "1" ) ) }
        s = try await WaitForSingle( declineSingle ) { $0.value == "2" }
        #expect( s.value == "2" )
        
        decline.send( "4" )
        try await Task.sleep( nanoseconds: 100_000_000 )
        #expect( declineSingle.entity?.value == "2" )
        
        decline.send( "5" )
        s = try await WaitForSingle( declineSingle ) { $0.value == "5" }
        #expect( s.value == "5" )
        
        let sixCollection = REEntityObservableCollectionExtra<TestEntity, ExtraCollectionParams>( queue: DispatchQueue( label: "test.combine.six" ) )
        let c0 = CurrentValueSubject<String, Never>( "0" )
        let c1 = CurrentValueSubject<String, Never>( "1" )
        let c2 = CurrentValueSubject<String, Never>( "2" )
        let c3 = CurrentValueSubject<String, Never>( "3" )
        let c4 = CurrentValueSubject<String, Never>( "4" )
        let c5 = CurrentValueSubject<String, Never>( "5" )
        sixCollection.combineLatest( c0, c1, c2, c3, c4, c5, apply: { $0.Modified( value: $1 + $2 + $3 + $4 + $5 + $6 ) } )
        let sixSingle = sixCollection.CreateSingleBack( key: "1" ) { _ in BackSingle( TestEntityBack( id: "1", value: "base" ) ) }
        s = try await WaitForSingle( sixSingle ) { $0.value == "012345" }
        #expect( s.value == "012345" )
        
        c5.send( "6" )
        s = try await WaitForSingle( sixSingle ) { $0.value == "012346" }
        #expect( s.value == "012346" )
    }
    
    @Test func combineLatestPaginatorDeclineAndInitialArray() async throws
    {
        let collection = REEntityObservableCollectionExtra<TestEntity, ExtraCollectionParams>( queue: DispatchQueue( label: "test.combine.pager" ) )
        let rxObs = CurrentValueSubject<String, Never>( "2" )
        let rxObs1 = CurrentValueSubject<String, Never>( "3" )
        collection.combineLatest( rxObs, apply: { $0.Modified( value: "\($0.id)\($1)" ) } )
        collection.combineLatest( rxObs1, apply: { $0.Modified( value: "\($0.id)\($1)" ) } )
        
        let pager = collection.CreatePaginatorBack( perPage: 2 )
        {
            params in
            
            if params.page == 0
            {
                return BackArray( [TestEntityBack( id: "1", value: "1" ), TestEntityBack( id: "2", value: "1" )] )
            }
            
            return BackArray( [TestEntityBack( id: "3", value: "1" ), TestEntityBack( id: "4", value: "1" )] )
        }
        
        var s = try await WaitForArray( pager ) { $0.count == 2 && $0[0].value == "13" }
        #expect( s[0].value == "13" )

        rxObs.send( "4" )
        rxObs1.send( "4" )
        s = try await WaitForArray( pager ) { $0.count == 2 && $0[0].value == "14" }
        #expect( s[0].value == "14" )

        rxObs1.send( "5" )
        s = try await WaitForArray( pager ) { $0.count == 2 && $0[0].value == "15" }
        #expect( s[0].value == "15" )

        pager.Next()
        s = try await WaitForArray( pager ) { $0.count == 4 && $0[2].value == "35" }
        #expect( s[2].value == "35" )
        
        let declineCollection = REEntityObservableCollectionExtra<TestEntity, ExtraCollectionParams>( queue: DispatchQueue( label: "test.combine.pager.decline" ) )
        let decline = CurrentValueSubject<String, Never>( "2" )
        declineCollection.combineLatest( decline, test: { $0.1 != "4" }, apply: { $0.0.Modified( value: "\($0.0.id)\($0.1)" ) } )
        let declinePager = declineCollection.CreatePaginatorBack( perPage: 2 )
        {
            params in
            
            params.page == 0 ? BackArray( [TestEntityBack( id: "1", value: "1" ), TestEntityBack( id: "2", value: "1" )] ) : BackArray( [TestEntityBack( id: "3", value: "1" ), TestEntityBack( id: "4", value: "1" )] )
        }
        
        s = try await WaitForArray( declinePager ) { $0.count == 2 && $0[0].value == "12" }
        decline.send( "4" )
        try await Task.sleep( nanoseconds: 100_000_000 )
        #expect( declinePager.entities[0].value == "12" )
        decline.send( "5" )
        s = try await WaitForArray( declinePager ) { $0.count == 2 && $0[0].value == "15" }
        #expect( s[0].value == "15" )
        declinePager.Next()
        s = try await WaitForArray( declinePager ) { $0.count == 4 && $0[2].value == "35" }
        #expect( s[2].value == "35" )
        
        let arrayCollection = REEntityObservableCollectionExtra<TestEntity, ExtraCollectionParams>( queue: DispatchQueue( label: "test.combine.array.initial" ), collectionExtra: ExtraCollectionParams( test: "2" ) )
        let a0 = CurrentValueSubject<String, Never>( "2" )
        let a1 = CurrentValueSubject<String, Never>( "3" )
        arrayCollection.combineLatest( a0, apply: { $0.Modified( value: "\($0.id)\($1)" ) } )
        arrayCollection.combineLatest( a1, apply: { $0.Modified( value: "\($0.id)\($1)" ) } )
        arrayCollection.arrayFetchCallback = { _ in EntityArray( [] ) }

        let array = arrayCollection.CreateKeyArray( initial: [TestEntity( id: "1", value: "2" ), TestEntity( id: "2", value: "3" ) ] )
        s = try await WaitForArray( array ) { $0.count == 2 && $0[0].value == "13" }
        #expect( s[0].value == "13" )

        a0.send( "4" )
        a1.send( "4" )
        s = try await WaitForArray( array ) { $0.count == 2 && $0[0].value == "14" }
        #expect( s[0].value == "14" )

        a1.send( "5" )
        s = try await WaitForArray( array ) { $0.count == 2 && $0[0].value == "15" }
        #expect( s[0].value == "15" )
    }
    
    @Test func singleStateLoadingAndKeyAssignment() async throws
    {
        var fetchCount = 0
        let collection = REEntityObservableCollectionExtra<TestEntity, ExtraCollectionParams>( queue: DispatchQueue( label: "test.single.state" ), collectionExtra: ExtraCollectionParams( test: "2" ) )
        let single = collection.CreateSingle( start: false )
        {
            params in
            
            fetchCount += 1
            return params.first ? DelayedEntitySingle( nil ) : DelayedEntitySingle( TestEntity( id: "1", value: "2" ) )
        }

        var state = try await WaitFor( single.rxState ) { $0 == .initializing }
        var loader = try await WaitFor( single.rxLoader ) { $0 == .none }
        #expect( state == .initializing )
        #expect( loader == .none )
        
        single.Refresh()
        loader = try await WaitFor( single.rxLoader ) { $0 == .firstLoading }
        #expect( loader == .firstLoading )
        state = try await WaitFor( single.rxState ) { $0 == .notFound }
        #expect( state == .notFound )
        
        single.Refresh()
        loader = try await WaitFor( single.rxLoader ) { $0 == .loading }
        #expect( loader == .loading )
        state = try await WaitFor( single.rxState ) { $0 == .ready }
        #expect( state == .ready )
        #expect( fetchCount == 2 )
        
        let keyCollection = REEntityObservableCollectionExtra<TestEntity, ExtraCollectionParams>( queue: DispatchQueue( label: "test.single.key" ), collectionExtra: ExtraCollectionParams( test: "2" ) )
        keyCollection.singleFetchCallback = { EntitySingle( TestEntity( id: $0.key!.stringKey, value: "2" ) ) }
        let keyed = keyCollection.CreateSingle()
        state = try await WaitFor( keyed.rxState ) { $0 == .initializing }
        loader = try await WaitFor( keyed.rxLoader ) { $0 == .none }
        #expect( state == .initializing )
        #expect( loader == .none )
        
        keyed.key = "1"
        let e = try await WaitForSingle( keyed ) { $0.id == "1" }
        #expect( keyed.entity?.id == "1" )
        #expect( e.value == "2" )
    }
    
    @Test func commits() async throws
    {
        let collection = REEntityObservableCollectionExtra<TestEntity, ExtraCollectionParams>( queue: DispatchQueue( label: "test.commits" ), collectionExtra: ExtraCollectionParams( test: "2" ) )

        collection.arrayFetchCallback = { _ in EntityArray( [] ) }
        collection.singleFetchCallback = { _ in EntitySingle( nil ) }

        let array = collection.CreateKeyArray( initial: [TestEntity( id: "1", value: "2" ), TestEntity( id: "2", value: "3" ) ] )
        _ = try await WaitForArray( array ) { $0.count == 2 }
        let single = collection.CreateSingle( key: "1" )
        _ = try await WaitForSingle( single ) { $0.value == "2" }
        
        collection.Commit( entity: TestEntity( id: "1", value: "12" ), operation: .update )
        var a = try await WaitForArray( array ) { $0.count == 2 && $0[0].value == "12" }
        var s = try await WaitForSingle( single ) { $0.value == "12" }
        #expect( s.value == "12" )
        #expect( a[0].value == "12" )
        
        collection.Commit( key: "1", changes: { TestEntity( id: $0.id, value: "13" ) } )
        a = try await WaitForArray( array ) { $0[0].value == "13" }
        s = try await WaitForSingle( single ) { $0.value == "13" }
        #expect( s.value == "13" )
        #expect( a[0].value == "13" )
        
        collection.Commit( keys: ["1", "2"], changes: { TestEntity( id: $0.id, value: "\($0.id)4" ) } )
        a = try await WaitForArray( array )
        {
            (items: [TestEntity]) in
            
            items.count == 2 && items[0].value == "14" && items[1].value == "24"
        }
        s = try await WaitForSingle( single ) { $0.value == "14" }
        #expect( s.value == "14" )
        #expect( a[1].value == "24" )
        
        let rxComb = CurrentValueSubject<Int, Never>( 1 )
        final class Flag: @unchecked Sendable
        {
            private let lock = NSLock()
            private var _called = false
            
            var called: Bool
            {
                get
                {
                    lock.lock()
                    defer { lock.unlock() }
                    
                    return _called
                }
                set
                {
                    lock.lock()
                    defer { lock.unlock() }
                    
                    _called = newValue
                }
            }
        }
        let flag = Flag()
        collection.combineLatest( rxComb, apply:
        {
            entity, _ in
            
            flag.called = entity.id == "commit"
            return entity.Modified( value: "" )
        } )
        
        collection.Commit( entities: [TestEntity( id: "commit", value: "1" )], operations: [.insert] )
        try await WaitUntil { flag.called }
        #expect( flag.called )
    }
    
    @Test func repositoriesUpdateDeleteClearAndRefresh() async throws
    {
        let repository = TestRepository<TestEntityBack>()
        repository.Add( entities: [TestEntityBack( id: "1", value: "test1" ), TestEntityBack( id: "2", value: "test2" )] )
        
        let collection = REEntityObservableCollectionExtra<TestEntity, ExtraCollectionParams>( queue: DispatchQueue( label: "test.repository" ), collectionExtra: ExtraCollectionParams( test: "2" ) )
        collection.repository = repository

        let allArray = collection.CreateArrayBack { _ in BackArray( repository.items ) }
        let array = collection.CreateKeyArray( keys: ["1", "2"] )
        let single = collection.CreateSingle( key: "1" )

        var a = try await WaitForArray( array ) { $0.count == 2 && $0[0].value == "test1" }
        var s = try await WaitForSingle( single ) { $0.value == "test1" }
        #expect( s.value == "test1" )
        #expect( a[1].value == "test2" )
        
        repository.Update( entity: TestEntityBack( id: "1", value: "test1-new" ) )
        a = try await WaitForArray( array ) { $0.count == 2 && $0[0].value == "test1-new" }
        s = try await WaitForSingle( single ) { $0.value == "test1-new" }
        #expect( s.value == "test1-new" )
        #expect( a[0].value == "test1-new" )
        
        repository.Delete( key: "1" )
        a = try await WaitForArray( array ) { $0.count == 1 && $0[0].id == "2" }
        let state = try await WaitFor( single.rxState ) { $0 == .deleted }
        #expect( state == .deleted )
        #expect( a[0].value == "test2" )
        
        repository.items.removeAll()
        allArray.Refresh()
        _ = try await WaitForArray( allArray ) { $0.isEmpty }
        
        let clearRepository = TestRepository<TestEntityBack>()
        clearRepository.Add( entities: [TestEntityBack( id: "1", value: "test1" ), TestEntityBack( id: "2", value: "test2" )] )
        let clearCollection = REEntityObservableCollectionExtra<TestEntity, ExtraCollectionParams>( queue: DispatchQueue( label: "test.repository.clear" ), collectionExtra: ExtraCollectionParams( test: "2" ) )
        clearCollection.repository = clearRepository

        let clearAllArray = clearCollection.CreateArrayBack { _ in BackArray( clearRepository.items ) }
        let clearArray = clearCollection.CreateKeyArray( keys: ["1", "2"] )
        let clearSingle = clearCollection.CreateSingle( key: "1" )
        #expect( clearAllArray.perPage == RE_ARRAY_PER_PAGE )
        _ = try await WaitForArray( clearArray ) { $0.count == 2 }

        clearRepository.Clear()
        a = try await WaitForArray( clearAllArray ) { $0.isEmpty }
        #expect( a.count == 0 )
        a = try await WaitForArray( clearArray ) { $0.isEmpty }
        #expect( a.count == 0 )
        let clearState = try await WaitFor( clearSingle.rxState ) { $0 == .deleted }
        #expect( clearState == .deleted )
        
        let refreshRepository = TestRepository<TestEntityBack>()
        refreshRepository.Add( entities: [TestEntityBack( id: "1", value: "test1" ), TestEntityBack( id: "2", value: "test2" )] )
        let refreshCollection = REEntityObservableCollectionExtra<TestEntity, ExtraCollectionParams>( queue: DispatchQueue( label: "test.repository.refresh" ), collectionExtra: ExtraCollectionParams( test: "2" ) )
        refreshCollection.repository = refreshRepository

        let refreshAllArray = refreshCollection.CreateArrayBack { _ in BackArray( refreshRepository.items ) }
        let refreshArray = refreshCollection.CreateKeyArray( keys: ["1", "2"] )
        _ = try await WaitForArray( refreshArray ) { $0.count == 2 }

        refreshRepository.items.removeAll()
        refreshCollection.Refresh()
        a = try await WaitForArray( refreshAllArray ) { $0.isEmpty }
        #expect( a.count == 0 )
        a = try await WaitForArray( refreshArray ) { $0.isEmpty }
        #expect( a.count == 0 )
    }
    
    @Test func repositoryConnectIndirectUpdates() async throws
    {
        let repositoryIndirect = TestRepositoryIndirect()
        repositoryIndirect.Add( entities: [IndirectEntityBack( id: "1", value: "indirect1" ), IndirectEntityBack( id: "2", value: "indirect2" )] )
        
        let repository = TestRepositoryDirect( second: repositoryIndirect )
        repository.Add( entities: [TestEntityBack( id: "1", value: "test1", indirectId: "2", indirectValue: "" ), TestEntityBack( id: "2", value: "test2", indirectId: "1", indirectValue: "" )] )
        
        repository.Connect( repository: repositoryIndirect, fieldPath: \TestEntity.indirectId )
        
        let collection = REEntityObservableCollectionExtra<TestEntity, ExtraCollectionParams>( queue: DispatchQueue( label: "test.repository.connect" ), collectionExtra: ExtraCollectionParams( test: "2" ) )
        collection.repository = repository

        let array = collection.CreateKeyArray( keys: ["1", "2"] )
        let single = collection.CreateSingle( key: "1" )
        
        var a = try await WaitForArray( array ) { $0.count == 2 && $0[0].indirectValue == "indirect2" }
        var s = try await WaitForSingle( single ) { $0.indirectValue == "indirect2" }
        #expect( s.indirectValue == "indirect2" )
        #expect( a[1].indirectValue == "indirect1" )
        
        repositoryIndirect.Update( entity: IndirectEntityBack( id: "1", value: "indirect-1" ) )
        repositoryIndirect.Update( entity: IndirectEntityBack( id: "2", value: "indirect-2" ) )
        
        a = try await WaitForArray( array )
        {
            (items: [TestEntity]) in
            
            items.count == 2 && items[0].indirectValue == "indirect-2" && items[1].indirectValue == "indirect-1"
        }
        s = try await WaitForSingle( single ) { $0.indirectValue == "indirect-2" }
        #expect( s.indirectValue == "indirect-2" )
        #expect( a[1].indirectValue == "indirect-1" )
    }
}
