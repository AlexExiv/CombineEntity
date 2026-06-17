// swift-tools-version: 6.0

import PackageDescription

let package = Package(
    name: "CombineEntity",
    platforms: [
        .iOS( .v16 ),
        .macOS( .v10_15 )
    ],
    products: [
        .library(
            name: "CombineEntity",
            targets: ["CombineEntity"]
        )
    ],
    targets: [
        .target(
            name: "CombineEntity",
            path: "CombineEntity",
            resources: [
                .process( "PrivacyInfo.xcprivacy" )
            ],
            swiftSettings: [
                .swiftLanguageMode( .v6 )
            ]
        ),
        .testTarget(
            name: "CombineEntityTests",
            dependencies: ["CombineEntity"],
            path: "CombineEntityTests",
            swiftSettings: [
                .swiftLanguageMode( .v6 )
            ]
        )
    ]
)
