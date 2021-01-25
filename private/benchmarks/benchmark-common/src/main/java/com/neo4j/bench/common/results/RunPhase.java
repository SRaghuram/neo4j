/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.results;

public enum RunPhase
{
    WARMUP( "_WARMUP" ),
    MEASUREMENT( "" );
    private final String nameModifier;

    RunPhase( String nameModifier )
    {
        this.nameModifier = nameModifier;
    }

    public String nameModifier()
    {
        return nameModifier;
    }
}
