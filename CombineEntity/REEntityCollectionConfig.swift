//
//  REEntityCollectionConfig.swift
//  CombineEntity
//
//  Created by ALEXEY ABDULIN on 11.08.2025.
//  Copyright © 2025 ALEXEY ABDULIN. All rights reserved.
//

import Foundation

public final class REEntityCollectionConfig: @unchecked Sendable
{
    public static let shared = REEntityCollectionConfig()
    
    private let lock = NSLock()
    private var _isLogging = false
    
    // Shared mutable logging state is guarded by `lock`.
    public static var isLogging: Bool
    {
        get
        {
            shared.lock.lock()
            defer { shared.lock.unlock() }
            
            return shared._isLogging
        }
        set
        {
            shared.lock.lock()
            defer { shared.lock.unlock() }
            
            shared._isLogging = newValue
        }
    }
    
    static public func Log( _ items: Any..., separator: String = " ", terminator: String = "\n" )
    {
        if isLogging
        {
            print( items, separator: separator, terminator: terminator )
        }
    }
}
