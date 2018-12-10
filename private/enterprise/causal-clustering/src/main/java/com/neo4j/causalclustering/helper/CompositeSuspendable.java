/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import org.neo4j.function.ThrowingConsumer;

public class CompositeSuspendable implements Suspendable
{
    private final List<Suspendable> suspendables = new ArrayList<>();

    public void add( Suspendable suspendable )
    {
        suspendables.add( suspendable );
    }

    @Override
    public void enable()
    {
        doOperation( Suspendable::enable, "Enable" );
    }

    @Override
    public void disable()
    {
        doOperation( Suspendable::disable, "Disable" );
    }

    private void doOperation( ThrowingConsumer<Suspendable,Throwable> operation, String description )
    {
        ErrorHandler.runAll( description, suspendables.stream()
                .map( (Function<Suspendable,ErrorHandler.ThrowingRunnable>) suspendable -> () -> operation.accept( suspendable ) )
                .toArray( ErrorHandler.ThrowingRunnable[]::new ) );
    }
}
