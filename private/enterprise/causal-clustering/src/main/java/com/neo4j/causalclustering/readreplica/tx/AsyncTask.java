/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import java.util.concurrent.Callable;

public class AsyncTask implements Runnable
{
    private final Callable<Void> task;
    private final Aborter aborter;
    private final FailureEventHandler failureEventHandler;

    public AsyncTask( Callable<Void> task, Aborter aborter, FailureEventHandler failureEventHandler )
    {
        this.task = task;
        this.aborter = aborter;
        this.failureEventHandler = failureEventHandler;
    }

    @Override
    public void run()
    {
        if ( aborter.shouldAbort() )
        {
            return;
        }
        try
        {
            task.call();
        }
        catch ( Exception e )
        {
            failureEventHandler.onFailure( e );
        }
    }
}
