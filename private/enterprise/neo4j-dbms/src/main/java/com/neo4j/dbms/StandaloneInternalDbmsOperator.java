/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.stream.Collectors;

import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.monitoring.PanicDatabaseEvent;
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
public class StandaloneInternalDbmsOperator extends DbmsOperator implements DatabaseEventListener
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
            log.warn( "Panic event received for %s but the provided cause is null, so this event was ignored.", databaseId );
            return;
        }

        shouldStop.add( databaseId );
        var reconcilerResult = trigger( ReconcilerRequest.panickedTarget( databaseId, causeOfPanic ).build() );
        reconcilerResult.whenComplete( () -> shouldStop.remove( databaseId ) );
    }

    @Override
    public void databaseStart( DatabaseEventContext eventContext )
    { //no-op
    }

    @Override
    public void databaseShutdown( DatabaseEventContext eventContext )
    { //no-op
    }

    @Override
    public void databasePanic( DatabaseEventContext eventContext )
    {
        PanicDatabaseEvent panic = (PanicDatabaseEvent) eventContext;
        stopOnPanic( panic.getDatabaseId(), panic.getCauseOfPanic() );
    }
}
