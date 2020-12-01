/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

import static org.neo4j.util.Preconditions.checkState;

/**
 * {@inheritDoc}
 * <p>
 * Important: For the purposes of panics the DBMS and individual databases are considered to be loosely coupled. This means that a DBMS panic does not cause a
 * panic of any databases. And vice-versa a database panic does not cause a DBMS panic, with the single exception that a system db panic will cause a DBMS panic
 * because the system db is an essential part of the DBMS.
 * <p>
 * While it is tempting to think that a DBMS panic should panic all the databases there's particular no reason to take the databases offline because of a
 * problem with the DBMS. While DBMS functions like creating and dropping database may no longer be possible regular interaction with already running databases
 * need not be affected and the user may want to continue to connect to those databases despite the DBMS panic.
 */
public class DefaultPanicService implements PanicService
{
    private final Map<NamedDatabaseId,DatabasePanicEventHandlers> handlersByDatabase = new ConcurrentHashMap<>();
    private final Executor executor;
    private final Log log;
    private final DbmsPanicker dbmsPanicker;

    public DefaultPanicService( JobScheduler jobScheduler, LogService logService, DbmsPanicker dbmsPanicker )
    {
        executor = jobScheduler.executor( Group.PANIC_SERVICE );
        log = logService.getUserLog( getClass() );
        this.dbmsPanicker = dbmsPanicker;
    }

    @Override
    public void addDatabasePanicEventHandlers( NamedDatabaseId namedDatabaseId, List<? extends DatabasePanicEventHandler> handlers )
    {
        var newHandlers = new DatabasePanicEventHandlers( handlers );
        var oldHandlers = handlersByDatabase.putIfAbsent( namedDatabaseId, newHandlers );
        checkState( oldHandlers == null, "Panic handlers for %s are already installed", namedDatabaseId );
    }

    @Override
    public void removeDatabasePanicEventHandlers( NamedDatabaseId namedDatabaseId )
    {
        handlersByDatabase.remove( namedDatabaseId );
    }

    @Override
    public Panicker panicker()
    {
        return this::panicAsync;
    }

    @Override
    public DatabasePanicker panickerFor( NamedDatabaseId namedDatabaseId )
    {
        return ( reason, error ) -> panicAsync( new DatabasePanicEvent( namedDatabaseId, reason, error ) );
    }

    private void panicAsync( Panicker.PanicEvent panicEvent )
    {
        executor.execute( () -> doPanic( panicEvent ) );
    }

    private void doPanic( Panicker.PanicEvent panicEvent )
    {
        // I wish we could do scala style matching here
        if ( panicEvent instanceof DatabasePanicEvent )
        {
            panicDatabase( (DatabasePanicEvent) panicEvent );
        }
        else if ( panicEvent instanceof DbmsPanicEvent )
        {

            panicDbms( (DbmsPanicEvent) panicEvent );
        }
        else
        {
            throw new IllegalStateException( "Unexpected panic event class: " + panicEvent.getClass()
                                             + " with reason: " + panicEvent.getReason().getDescription(),
                                             panicEvent.getCause() );
        }
    }

    private void panicDatabase( DatabasePanicEvent genericPanicEvent )
    {
        var panic = (DatabasePanicEvent) genericPanicEvent;
        log.error( "Components for '" + panic.databaseId() + "' have encountered a critical error: " + panic.getReason().getDescription(),
                   panic.getCause() );

        var handlers = handlersByDatabase.get( panic.databaseId() );
        if ( handlers != null )
        {
            handlers.handleDatabasePanic( panic );
        }
    }

    private void panicDbms( DbmsPanicEvent panicEvent )
    {
        log.error( "Components for the Database Management System (DBMS) have encountered a critical error: " + panicEvent.getReason().getDescription(),
                   panicEvent.getCause() );
        dbmsPanicker.panic( (DbmsPanicEvent) panicEvent );
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

        void handleDatabasePanic( DatabasePanicEvent panicInfo )
        {
            if ( panicked.compareAndSet( false, true ) )
            {
                for ( var handler : handlers )
                {
                    try
                    {
                        handler.onPanic( panicInfo );
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
