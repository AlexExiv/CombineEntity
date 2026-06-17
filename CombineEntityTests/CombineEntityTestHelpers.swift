//
//  CombineEntityTestHelpers.swift
//  CombineEntityTests
//
//  Created by Codex on 17.06.2026.
//

import Foundation
import Combine
import Testing
@testable import CombineEntity

struct WaitTimeout: Error
{
    
}

final class PublisherWaiter<Value: Sendable>: @unchecked Sendable
{
    private let lock = NSLock()
    private var cancellable: AnyCancellable?
    private var completed = false
    
    func set( _ cancellable: AnyCancellable )
    {
        lock.lock()
        defer { lock.unlock() }
        
        self.cancellable = cancellable
    }
    
    func succeed( _ value: Value, _ continuation: CheckedContinuation<Value, Error> )
    {
        lock.lock()
        guard !completed else
        {
            lock.unlock()
            return
        }
        completed = true
        let cancellable = self.cancellable
        self.cancellable = nil
        lock.unlock()
        
        cancellable?.cancel()
        continuation.resume( returning: value )
    }
    
    func fail( _ error: Error, _ continuation: CheckedContinuation<Value, Error> )
    {
        lock.lock()
        guard !completed else
        {
            lock.unlock()
            return
        }
        completed = true
        let cancellable = self.cancellable
        self.cancellable = nil
        lock.unlock()
        
        cancellable?.cancel()
        continuation.resume( throwing: error )
    }
    
    func cancel()
    {
        lock.lock()
        let cancellable = self.cancellable
        self.cancellable = nil
        completed = true
        lock.unlock()
        
        cancellable?.cancel()
    }
}

func WaitFor<P: Publisher>( _ publisher: P, timeout: TimeInterval = 2.0, where predicate: @escaping (P.Output) -> Bool = { _ in true } ) async throws -> P.Output where P.Failure == Never, P.Output: Sendable
{
    let waiter = PublisherWaiter<P.Output>()
    
    return try await withTaskCancellationHandler
    {
        try await withCheckedThrowingContinuation
        {
            (continuation: CheckedContinuation<P.Output, Error>) in
            
            waiter.set( publisher.sink
            {
                value in
                
                if predicate( value )
                {
                    waiter.succeed( value, continuation )
                }
            } )
            
            Task
            {
                try? await Task.sleep( nanoseconds: UInt64( timeout * 1_000_000_000 ) )
                waiter.fail( WaitTimeout(), continuation )
            }
        }
    } onCancel:
    {
        waiter.cancel()
    }
}

func WaitSingle<P: Publisher>( _ publisher: P, timeout: TimeInterval = 2.0 ) async throws -> P.Output where P.Failure == Error, P.Output: Sendable
{
    let waiter = PublisherWaiter<P.Output>()
    
    return try await withTaskCancellationHandler
    {
        try await withCheckedThrowingContinuation
        {
            (continuation: CheckedContinuation<P.Output, Error>) in
            
            waiter.set( publisher.sink( receiveCompletion:
            {
                completion in
                
                if case let .failure( error ) = completion
                {
                    waiter.fail( error, continuation )
                }
            }, receiveValue:
            {
                value in
                
                waiter.succeed( value, continuation )
            } ) )
            
            Task
            {
                try? await Task.sleep( nanoseconds: UInt64( timeout * 1_000_000_000 ) )
                waiter.fail( WaitTimeout(), continuation )
            }
        }
    } onCancel:
    {
        waiter.cancel()
    }
}

func WaitForSingle<Entity: REEntity, Extra>( _ single: RESingleObservableExtra<Entity, Extra>, timeout: TimeInterval = 2.0, where predicate: @escaping (Entity) -> Bool = { _ in true } ) async throws -> Entity
{
    let value = try await WaitFor( single, timeout: timeout ) { $0.map( predicate ) ?? false }
    return try #require( value )
}

func WaitForArray<Entity: REEntity, Extra>( _ array: REArrayObservableExtra<Entity, Extra>, timeout: TimeInterval = 2.0, where predicate: @escaping ([Entity]) -> Bool ) async throws -> [Entity]
{
    try await WaitFor( array, timeout: timeout, where: predicate )
}

func WaitUntil( timeout: TimeInterval = 2.0, where predicate: @escaping @Sendable () -> Bool ) async throws
{
    let deadline = Date().addingTimeInterval( timeout )
    
    while Date() < deadline
    {
        if predicate()
        {
            return
        }
        
        try await Task.sleep( nanoseconds: 10_000_000 )
    }
    
    throw WaitTimeout()
}
