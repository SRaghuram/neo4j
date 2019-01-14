/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.error_handling;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.util.VisibleForTesting;

public class PanicService implements Panicker
{
    static final String PANIC_MESSAGE = "System has panicked.";
    private final List<PanicEventHandler> eventHandlers = new CopyOnWriteArrayList<>();
    private final PanicEventExecutor panicEventExecutor = new PanicEventExecutor();
    private final Thread thread = new Thread( panicEventExecutor, "panic-thread" );
    private final Log log;
    private final boolean blocking;
    private final AtomicBoolean hasPanicked = new AtomicBoolean();

    public PanicService( LogProvider logProvider )
    {
        this( logProvider, false );
    }

    @VisibleForTesting
    PanicService( LogProvider logProvider, boolean blocking )
    {
        thread.setDaemon( true );
        this.log = logProvider.getLog( getClass() );
        this.blocking = blocking;
    }

    public void addPanicEventHandler( PanicEventHandler panicEventHandler )
    {
        eventHandlers.add( panicEventHandler );
    }

    @Override
    public void panic( Throwable e )
    {
        if ( hasPanicked.compareAndSet( false, true ) )
        {
            tryLog( PANIC_MESSAGE, e );
            thread.start();
            possiblyBlock();
        }
    }

    private void tryLog( String message, Throwable e )
    {
        try
        {
            if ( e == null )
            {
                log.error( message );
            }
            else
            {
                log.error( message, e );
            }
        }
        catch ( Throwable t )
        {
            System.err.println( message );
            if ( e != null )
            {
                e.printStackTrace();
            }
        }
    }

    private void possiblyBlock()
    {
        if ( blocking )
        {
            try
            {
                thread.join();
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    private class PanicEventExecutor implements Runnable
    {
        private static final String FAIL_MESSAGE = "Failed to handle panic event";

        @Override
        public void run()
        {
            for ( PanicEventHandler eventHandler : eventHandlers )
            {
                try
                {
                    eventHandler.onPanic();
                }
                catch ( Throwable t )
                {
                    tryLog( FAIL_MESSAGE, t );
                }
            }
        }
    }
}
