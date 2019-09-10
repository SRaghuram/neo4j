/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.util.Preconditions.checkState;

public class PanicService
{
    private final Map<DatabaseId,DatabasePanicEventHandlers> handlersByDatabase = new ConcurrentHashMap<>();
    private final Executor executor;
    private final Log log;

    public PanicService( JobScheduler jobScheduler, LogService logService )
    {
        executor = jobScheduler.executor( Group.PANIC_SERVICE );
        log = logService.getUserLog( getClass() );
    }

    public void addPanicEventHandlers( DatabaseId databaseId, List<? extends DatabasePanicEventHandler> handlers )
    {
        var newHandlers = new DatabasePanicEventHandlers( handlers );
        var oldHandlers = handlersByDatabase.putIfAbsent( databaseId, newHandlers );
        checkState( oldHandlers == null, "Panic handlers for database %s are already installed", databaseId.name() );
    }

    public void removePanicEventHandlers( DatabaseId databaseId )
    {
        handlersByDatabase.remove( databaseId );
    }

    public DatabasePanicker panickerFor( DatabaseId databaseId )
    {
        return error -> panicAsync( databaseId, error );
    }

    private void panicAsync( DatabaseId databaseId, Throwable error )
    {
        executor.execute( () -> panic( databaseId, error ) );
    }

    private void panic( DatabaseId databaseId, Throwable error )
    {
        log.error( "Clustering components for database '" + databaseId.name() + "' have encountered a critical error", error );

        var handlers = handlersByDatabase.get( databaseId );
        if ( handlers != null )
        {
            handlers.handlePanic( error );
        }
    }

    private class DatabasePanicEventHandlers
    {
        final List<? extends DatabasePanicEventHandler> handlers;
        final AtomicBoolean panicked;

        DatabasePanicEventHandlers( List<? extends DatabasePanicEventHandler> handlers )
        {
            this.handlers = handlers;
            this.panicked = new AtomicBoolean();
        }

        void handlePanic( Throwable cause )
        {
            if ( panicked.compareAndSet( false, true ) )
            {
                for ( var handler : handlers )
                {
                    try
                    {
                        handler.onPanic( cause );
                    }
                    catch ( Throwable t )
                    {
                        log.error( "Failed to handle a panic event", t );
                    }
                }
            }
        }
    }
}
