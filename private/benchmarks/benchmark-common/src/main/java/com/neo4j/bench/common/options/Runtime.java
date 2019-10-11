/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.common.options;

public enum Runtime
{
    DEFAULT,
    INTERPRETED,
    SLOTTED,
    LEGACY_COMPILED,
    MORSEL,
    PARALLEL;

    public String value()
    {
        return name().toLowerCase();
    }
}
