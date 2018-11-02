/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.readreplica;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.MINUTES;

class WaitForUpToDateStore extends LifecycleAdapter
{
    private final CatchupPollingProcess catchupProcess;
    private final Log log;

    WaitForUpToDateStore( CatchupPollingProcess catchupProcess, LogProvider logProvider )
    {
        this.catchupProcess = catchupProcess;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void start() throws Throwable
    {
        waitForUpToDateStore();
    }

    private void waitForUpToDateStore() throws InterruptedException, ExecutionException
    {
        boolean upToDate = false;
        do
        {
            try
            {
                upToDate = catchupProcess.upToDateFuture().get( 1, MINUTES );
            }
            catch ( TimeoutException e )
            {
                log.warn( "Waiting for up-to-date store. State: " + catchupProcess.describeState() );
            }
        }
        while ( !upToDate );
    }
}
