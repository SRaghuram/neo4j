/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import java.util.concurrent.Semaphore;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

abstract class ClientSequence
{
    protected final Log log;
    protected final BareClient clientHandler;
    private Semaphore semaphore;

    ClientSequence( LogProvider logProvider, BareClient clientHandler )
    {
        this.log = logProvider.getLog( getClass() );
        this.clientHandler = clientHandler;
    }

    ClientSequence start( String databaseName )
    {
        semaphore = new Semaphore( 0 );
        clientHandler.registerErrorCallback( this::finishWithError );
        firstStep( databaseName );
        return this;
    }

    void waitForFinish() throws InterruptedException
    {
        semaphore.acquire();
    }

    protected abstract void firstStep( String databaseName );

    protected final void finish()
    {
        clientHandler.registerErrorCallback( null );
        semaphore.release();
    }

    private void finishWithError( Throwable t )
    {
        log.error( "Exception occurred during send", t );
        semaphore.release();
    }
}
