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
    private final AsyncTaskEventHandler asyncTaskEventHandler;

    AsyncTask( Callable<Void> task, Aborter aborter, AsyncTaskEventHandler asyncTaskEventHandler )
    {
        this.task = task;
        this.aborter = aborter;
        this.asyncTaskEventHandler = asyncTaskEventHandler;
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
            asyncTaskEventHandler.onSuccess();
        }
        catch ( Exception e )
        {
            asyncTaskEventHandler.onFailure( e );
        }
    }
}
