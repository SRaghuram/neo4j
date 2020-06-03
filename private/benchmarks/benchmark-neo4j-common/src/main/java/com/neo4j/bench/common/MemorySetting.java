/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common;

public enum MemorySetting
{
    OFF_HEAP,
    ON_HEAP,
    DEFAULT;

    public static MemorySetting getSetting( String name )
    {
        return MemorySetting.valueOf( name.toUpperCase() );
    }
}
