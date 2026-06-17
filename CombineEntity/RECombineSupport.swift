//
//  RECombineSupport.swift
//  CombineEntity
//
//  Created by Codex on 17.06.2026.
//

import Foundation
import Combine

public typealias RESingle<Value> = AnyPublisher<Value, Error>

public func REJust<Value>( _ value: Value ) -> RESingle<Value>
{
    Just( value )
        .setFailureType( to: Error.self )
        .eraseToAnyPublisher()
}

public func REFail<Value>( _ error: Error ) -> RESingle<Value>
{
    Fail( error: error )
        .eraseToAnyPublisher()
}

public func REDeferred<Value>( _ work: @escaping ( @escaping (Result<Value, Error>) -> Void ) -> Void ) -> RESingle<Value>
{
    Deferred
    {
        Future<Value, Error>
        {
            promise in
            
            work( promise )
        }
    }
    .eraseToAnyPublisher()
}
