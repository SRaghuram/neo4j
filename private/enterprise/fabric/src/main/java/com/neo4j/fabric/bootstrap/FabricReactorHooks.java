/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bootstrap;

import reactor.core.publisher.Hooks;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class FabricReactorHooks
{
    static void register( LogProvider internalLogProvider )
    {
        Log log = internalLogProvider.getLog( FabricReactorHooks.class );
        Hooks.onErrorDropped( throwable -> log.error( "Dropped error in stream", throwable ) );
    }
}
