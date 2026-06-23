//
//  RERepository.swift
//  CombineEntity
//
//  Created by ALEXEY ABDULIN on 13.09.2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation
import Combine

public enum REUpdateOperation: Sendable
{
    case none, insert, update, delete, clear
}

public struct REEntityUpdated: CustomStringConvertible
{
    public let key: REEntityKey
    public let fieldPath: AnyKeyPath?
    public let entity: (any REBackEntityProtocol)?
    public let operation: REUpdateOperation
    
    public init( key: REEntityKey, fieldPath: AnyKeyPath? = nil, entity: (any REBackEntityProtocol)? = nil, operation: REUpdateOperation = .none )
    {
        self.key = key
        self.entity = entity
        self.operation = operation
        self.fieldPath = fieldPath
    }
    
    public init( entity: any REBackEntityProtocol, fieldPath: AnyKeyPath? = nil, operation: REUpdateOperation = .none )
    {
        self.init( key: entity.reKey, fieldPath: fieldPath, entity: entity, operation: operation )
    }
    
    public var description: String
    {
        return "Key: \(key.base); FieldPath: \(fieldPath == nil ? "nil" : "\(fieldPath!)"); EntityExist: \(entity != nil); Operation: \(operation)"
    }
}

public protocol REEntityRepositoryProtocol: AnyObject
{
    var rxEntitiesUpdated: PassthroughSubject<[REEntityUpdated], Never> { get }
    
    func _RxGet( key: REEntityKey ) -> RESingle<(any REBackEntityProtocol)?>
    func _RxGet( keys: [REEntityKey] ) -> RESingle<[any REBackEntityProtocol]>
}

public protocol REEntityAllRepositoryProtocol: REEntityRepositoryProtocol
{
    func _RxFetchAll() -> RESingle<[any REBackEntityProtocol]>
}

open class REEntityRepository<EntityBack: REBackEntityProtocol>: REEntityRepositoryProtocol
{
    public var rxEntitiesUpdated = PassthroughSubject<[REEntityUpdated], Never>()
    public var cancellables = Set<AnyCancellable>()
    
    public init()
    {
        
    }
    
    public func Connect<Entity: REEntity, V>( repository: REEntityRepositoryProtocol, fieldPath: KeyPath<Entity, V> )
    {
        repository
            .rxEntitiesUpdated
            .map { $0.map { REEntityUpdated( key: $0.key, fieldPath: fieldPath, operation: $0.operation ) } }
            .sink
            {
                [weak self] in
                
                self?.rxEntitiesUpdated.send( $0 )
            }
            .store( in: &cancellables )
    }
    
    public func _RxGet( key: REEntityKey ) -> RESingle<(any REBackEntityProtocol)?>
    {
        return RxGet( key: key )
            .map { $0.map { $0 as any REBackEntityProtocol } }
            .eraseToAnyPublisher()
    }
    
    public func _RxGet( keys: [REEntityKey] ) -> RESingle<[any REBackEntityProtocol]>
    {
        return RxGet( keys: keys )
            .map { $0.map { $0 as any REBackEntityProtocol } }
            .eraseToAnyPublisher()
    }
    
    open func RxGet( key: REEntityKey ) -> RESingle<EntityBack?>
    {
        preconditionFailure( "RxGet must be implemented" )
    }
    
    open func RxGet( keys: [REEntityKey] ) -> RESingle<[EntityBack]>
    {
        preconditionFailure( "RxGet must be implemented" )
    }
}
