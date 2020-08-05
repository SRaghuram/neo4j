/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.helper;

import com.neo4j.causalclustering.stresstests.Control;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public abstract class Workload implements Runnable
{
    protected final Control control;
    protected final ExecutorService executor;

    public Workload( Control control )
    {
        this.control = control;
        executor = Executors.newWorkStealingPool();
    }

    @Override
    public final void run()
    {
        try
        {
            while ( control.keepGoing() )
            {
                doWork();
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
        catch ( Throwable t )
        {
            control.onFailure( t );
        }
        finally
        {
            shutdownExecutor();
        }
    }

    private void shutdownExecutor()
    {
        executor.shutdownNow();
        try
        {
            if ( !executor.awaitTermination( 5, TimeUnit.MINUTES ) )
            {
                throw new RuntimeException( "Did not shut down workload executor within timeout." );
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
        }
    }

    protected abstract void doWork() throws Exception;

    protected final CompletableFuture<Void> runAsync( Runnable runnable )
    {
        return CompletableFuture.runAsync( runnable, executor );
    }

    protected final <T> CompletableFuture<T> supplyAsync( Supplier<T> supplier )
    {
        return CompletableFuture.supplyAsync( supplier, executor );
    }

    @SuppressWarnings( "RedundantThrows" )
    public void prepare() throws Exception
    {
    }

    @SuppressWarnings( "RedundantThrows" )
    public void validate() throws Exception
    {
    }
}
