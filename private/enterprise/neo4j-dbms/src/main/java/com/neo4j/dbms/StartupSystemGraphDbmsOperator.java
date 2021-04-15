/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

/**
 * Operator responsible for transitioning all databases known in the system graph to the desired state
 * (in most cases STARTED) at startup.
 * The system database should be reconciled as first.
 */
class StartupSystemGraphDbmsOperator extends DbmsOperator
{
    private final int startupChunk;
    private final EnterpriseSystemGraphDbmsModel dbmsModel;
    private final Log log;
    private final Map<String, EnterpriseDatabaseState> desired;

    StartupSystemGraphDbmsOperator( EnterpriseSystemGraphDbmsModel dbmsModel, Config config, LogProvider logProvider )
    {
        int parallelism = config.get( GraphDatabaseSettings.reconciler_maximum_parallelism );
        if ( parallelism == 0 )
        {
            parallelism = Runtime.getRuntime().availableProcessors();
        }
        this.startupChunk = parallelism;
        this.dbmsModel = dbmsModel;
        this.log = logProvider.getLog( getClass() );
        this.desired = new ConcurrentHashMap<>();
    }

    @Override
    protected Map<String,EnterpriseDatabaseState> desired0()
    {
        return desired;
    }

    void startSystem()
    {
        // Initially trigger system operator to start system db, it always desires the system db to be STARTED
        log.info( "Starting up '%s' database", NAMED_SYSTEM_DATABASE_ID.name() );
        // Only system is needed to be reconciled
        desired.put( NAMED_SYSTEM_DATABASE_ID.name(), new EnterpriseDatabaseState( NAMED_SYSTEM_DATABASE_ID, STARTED ) );
        trigger( ReconcilerRequest.simple() ).join( NAMED_SYSTEM_DATABASE_ID );
        log.info( "'%s' database started", NAMED_SYSTEM_DATABASE_ID.name() );
    }

    void startAllNonSystem()
    {
        // Manually kick off the reconciler to start all other databases in the system database, now that the system database is started
        // We do this ordered in a batch providing that on all eventual cluster members the same databases are started
        var allDatabases = dbmsModel.getDatabaseStates().values().stream()
                                    .filter( state -> !state.databaseId().equals( NAMED_SYSTEM_DATABASE_ID ) );
        var batches = batchDatabasesToStart( allDatabases );

        batches.forEach( databaseBatch ->
        {
            var desiredUpdate = databaseBatch.stream()
                                             .collect( Collectors.toMap( state -> state.databaseId().name(), Function.identity() ) );
            var batchLogMessage = desiredUpdate.values().stream()
                                               .map( EnterpriseDatabaseState::toShortString )
                                               .collect( Collectors.joining( ", " ) );
            var batchNames = databaseBatch.stream()
                                          .map( EnterpriseDatabaseState::databaseId )
                                          .collect( Collectors.toSet() );

            log.info( "Reconciling databases to desired states at startup: %s", batchLogMessage );
            // Only these databases are needed to be reconciled
            desired.putAll( desiredUpdate );
            trigger( ReconcilerRequest.simple() ).await( batchNames );
        } );

        log.info( "All databases were reconciled at startup" );
    }

    private Stream<List<EnterpriseDatabaseState>> batchDatabasesToStart( Stream<EnterpriseDatabaseState> states )
    {
        var idx = new AtomicInteger( 0 );

        var groups = states.sorted( Comparator.comparing( state -> state.databaseId().name() ) )
                .collect( Collectors.groupingBy( ignored -> idx.getAndIncrement() / startupChunk ) );

        return new TreeMap<>( groups ).values().stream();
    }
}
