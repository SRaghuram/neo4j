/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

import org.neo4j.kernel.monitoring.InternalDatabaseEventListener;
import org.neo4j.kernel.monitoring.PanicDatabaseEvent;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.monitoring.StartDatabaseEvent;
import org.neo4j.kernel.monitoring.StopDatabaseEvent;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.DatabaseHealth;

import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;

/**
 * Database operator for Standalone databases exposing state transitions needed by internal components.
 *
 * Initially this is only registered as an event handler to detect {@code panic} events in {@link DatabaseHealth}
 * classes.
 */
public class StandaloneInternalDbmsOperator extends DbmsOperator implements InternalDatabaseEventListener
{
    private final Set<NamedDatabaseId> shouldStop = new CopyOnWriteArraySet<>();
    private final Log log;

    StandaloneInternalDbmsOperator( LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    protected Map<String,EnterpriseDatabaseState> desired0()
    {
        return shouldStop.stream()
                         .map( id -> Pair.of( id.name(), new EnterpriseDatabaseState( id, STOPPED ) ) )
                         .collect( Collectors.toMap( Pair::first, Pair::other ) );
    }

    public void stopOnPanic( NamedDatabaseId databaseId, Throwable causeOfPanic )
    {
        if ( causeOfPanic == null )
        {
            log.warn( "Panic event received for the database %s but the provided cause is null, so this event was ignored.", databaseId.name() );
            return;
        }

        shouldStop.add( databaseId );
        var reconcilerResult = trigger( ReconcilerRequest.forPanickedDatabase( databaseId, causeOfPanic ) );
        reconcilerResult.whenComplete( () -> shouldStop.remove( databaseId ) );
    }

    @Override
    public void databaseStart( StartDatabaseEvent startDatabaseEvent )
    { //no-op
    }

    @Override
    public void databaseShutdown( StopDatabaseEvent stopDatabaseEvent )
    { //no-op
    }

    @Override
    public void databasePanic( PanicDatabaseEvent panic )
    {
        stopOnPanic( panic.getDatabaseId(), panic.getCauseOfPanic() );
    }
}
