//
//  REPaginatorObservable.swift
//  CombineEntity
//
//  Created by ALEXEY ABDULIN on 22/01/2020.
//  Copyright © 2020 ALEXEY ABDULIN. All rights reserved.
//

import Foundation

public class REPaginatorObservableExtra<Entity: REEntity, Extra>: REArrayObservableExtra<Entity, Extra>
{
    override init( holder: REEntityCollection<Entity>, extra: Extra? = nil, perPage: Int = RE_ARRAY_PER_PAGE, start: Bool = true, observeOn: DispatchQueue )
    {
        super.init( holder: holder, extra: extra, perPage: perPage, start: start, observeOn: observeOn )
    }
    
    /// Request next page if the observable has started or initializing otherwise
    public func Next()
    {
        
    }
    
    override func Append( entities: [Entity] ) -> [Entity]
    {
        lock.lock()
        defer { lock.unlock() }
        
        if perPage == RE_ARRAY_PER_PAGE
        {
            return super.Append( entities: entities )
        }
        
        var _entities = self.entities
        _entities.AppendOrReplace( entities: entities )
        Set( page: entities.count >= perPage ? page + 1 : PAGINATOR_END )
        return _entities
    }
}

public typealias REPaginatorObservable<Entity: REEntity> = REPaginatorObservableExtra<Entity, REEntityExtraParamsEmpty>
