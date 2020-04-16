/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.bootstrap;

import reactor.core.publisher.Hooks;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

class FabricReactorHooksService extends LifecycleAdapter
{
    // DESIGN NOTES:
    // Only one Neo4j server should run in a single JVM in most deployments
    // and only one error consumer will be registered in such cases.
    // However, if more than one Neo4j server run in a single JVM an error consumer
    // will be registered for each of them. This means that if an error in a Reactor stream is dropped
    // in any of them, the error will be reported in a log of every server.
    // This cannot be prevented as the error cannot be linked to an instance where it occurred.
    // This is not such a big problem as this type of errors signify Fabric bugs and, ideally, should not happen.
    // And if they occur reporting them in every Neo4j server on a JVM is not such a problem.
    // The hook is never unregistered from Reactor, because the API does not allow unregistering a concrete hook,
    // but provides only an operation that removes all of them. If that operation was used, it would mean removing
    // hooks potentially added by the users outside Neo4j code base for instance in extensions or even applications
    // if the server is used in embedded mode.

    private static final Set<Consumer<? super Throwable>> ERROR_CONSUMERS = ConcurrentHashMap.newKeySet();

    static
    {
        Hooks.onErrorDropped( e -> ERROR_CONSUMERS.forEach( consumer -> consumer.accept( e ) ) );
    }

    private final Consumer<? super Throwable> errorConsumer;

    FabricReactorHooksService( LogProvider internalLogProvider )
    {
        Log log = internalLogProvider.getLog( FabricReactorHooksService.class );
        errorConsumer = throwable -> log.error( "Dropped error in stream", throwable );
        ERROR_CONSUMERS.add( errorConsumer );
    }

    @Override
    public void stop()
    {
        ERROR_CONSUMERS.remove( errorConsumer );
    }
}
