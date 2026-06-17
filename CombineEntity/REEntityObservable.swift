//
//  REObjectObservable.swift
//  CombineEntity
//
//  Created by ALEXEY ABDULIN on 22/01/2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation
import Combine

public struct REEntityExtraParamsEmpty: Sendable
{
    
}

public class REEntityObservable<Entity: REEntity>
{
    //Loading state of the entity
    public enum Loading: Sendable
    {
        ///`none` -  entity hasn't loaded yet
        case none
        ///`firstLoading` - entity is loading first time
        case firstLoading
        ///`loading` - entity is loading due to refresh or something else
        case loading
        
        public var isLoading: Bool { self == .firstLoading || self == .loading }
    }
    
    public let rxSuspended = CurrentValueSubject<Bool, Never>( false )
    
    ///Contains loading state
    public let rxLoader = CurrentValueSubject<Loading, Never>( .none )
    ///Emits error in case of any error has occured during the entity loading. Use it to show an error alert or toast.
    public let rxError = PassthroughSubject<Error, Never>()
    ///Contains the last error state. If nil the last fetching was successful. Use it to show an error like overlay.
    public let rxLastError = CurrentValueSubject<Error?, Never>( nil )
    
    var cancellables = Set<AnyCancellable>()
    
    public let uuid = UUID().uuidString
    let lock = NSRecursiveLock()
    
    public private(set) weak var collection: REEntityCollection<Entity>? = nil
    
    init( holder: REEntityCollection<Entity> )
    {
        self.collection = holder
        holder.Add( object: self )
    }
    
    deinit
    {
        collection?.Remove( object: self )
        REEntityCollectionConfig.Log( "EntityObservable has been deleted. UUID - \(uuid)" )
    }
    
    /// Suspend updating of data. After that `EntityObservable` frezees requesting of the updating of the data when it gets update event.
    public func Suspend()
    {
        rxSuspended.send( true )
    }
    
    /// Resume updating of data. After that `EntityObservable` resumes requesting of the updating of the data when it gets update event. If update event has been received during the suspending time It requests update immidiately with the last passed extra params
    public func Resume()
    {
        rxSuspended.send( false )
        ResumeData()
    }
    
    func ResumeData()
    {
        
    }

    func Update( source: String, entity: Entity )
    {
        
    }
    
    func Update( source: String, entities: [REEntityKey: Entity] )
    {
        
    }
    
    func Update( entity: Entity, operation: REUpdateOperation )
    {
        Update( entities: [entity._key: entity], operation: operation )
    }

    func Update( entities: [REEntityKey: Entity], operation: REUpdateOperation )
    {
        fatalError( "This method must be overridden" )
    }
    
    func Update( entities: [REEntityKey: Entity], operations: [REEntityKey: REUpdateOperation] )
    {
        fatalError( "This method must be overridden" )
    }
    
    func Delete( keys: Set<REEntityKey> )
    {
        fatalError( "This method must be overridden" )
    }
    
    func Clear()
    {
        fatalError( "This method must be overridden" )
    }
    
    func RefreshData( resetCache: Bool, data: Any? )
    {
        
    }
    
    func Store( _ cancellable: AnyCancellable )
    {
        lock.lock()
        defer { lock.unlock() }
        
        cancellables.insert( cancellable )
    }
}

extension REEntityObservable
{
    public func bind( loader: CurrentValueSubject<REEntityObservable.Loading, Never> ) -> AnyCancellable
    {
        return rxLoader
            .sink { loader.send( $0 ) }
    }
    
    public func bind( error: PassthroughSubject<Error, Never> ) -> AnyCancellable
    {
        return rxError
            .sink { error.send( $0 ) }
    }
}
