/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.util.function.Supplier;

public class TestFirstStartupDetector implements DiscoveryFirstStartupDetector
{
    private final Supplier<Boolean> supplier;

    public TestFirstStartupDetector( Supplier<Boolean> skip )
    {
        this.supplier = skip;
    }

    public TestFirstStartupDetector( Boolean skip )
    {
        this.supplier = () -> skip;
    }

    @Override
    public Boolean isFirstStartup()
    {
        return supplier.get();
    }
}
