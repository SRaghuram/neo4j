/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;


import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

/**
 * Operator responsible for transitioning all non-DROPPED databases to a STOPPED state.
 * The system database is reconciled only after all other databases have successfully stopped.
 */
class ShutdownOperator extends DbmsOperator
{
    private final DatabaseManager<?> databaseManager;
    private final int shutdownChunk;

    ShutdownOperator( DatabaseManager<?> databaseManager, Config config )
    {
        this.databaseManager = databaseManager;

        int parallelism = config.get( GraphDatabaseSettings.reconciler_maximum_parallelism );
        if ( parallelism == 0 )
        {
            parallelism = Runtime.getRuntime().availableProcessors();
        }

        this.shutdownChunk = parallelism;
    }

    void stopAll()
    {
        desired.clear();
        var allDbsNoSystem = databaseManager.registeredDatabases().keySet().stream()
                .filter( e -> !e.equals( NAMED_SYSTEM_DATABASE_ID ) );

        var batches = batchDatabasesToStop( allDbsNoSystem );

        batches.forEach( databaseBatch ->
        {
            var desiredUpdate = databaseBatch.stream().collect( Collectors.toMap( NamedDatabaseId::name, this::stoppedState ) );
            desired.putAll( desiredUpdate );
            trigger( ReconcilerRequest.priorityTargets( databaseBatch ).build() ).await( databaseBatch );
        } );

        desired.put( NAMED_SYSTEM_DATABASE_ID.name(), stoppedState( NAMED_SYSTEM_DATABASE_ID ) );
        trigger( ReconcilerRequest.priorityTarget( NAMED_SYSTEM_DATABASE_ID ).build() ).await( NAMED_SYSTEM_DATABASE_ID );
    }

    Stream<Set<NamedDatabaseId>> batchDatabasesToStop( Stream<NamedDatabaseId> databases )
    {
        var idx = new AtomicInteger( 0 );

        return databases.collect( Collectors.groupingBy( ignored -> idx.getAndIncrement() / shutdownChunk ) )
                .values().stream()
                .map( HashSet::new );
    }

    private EnterpriseDatabaseState stoppedState( NamedDatabaseId id )
    {
        return new EnterpriseDatabaseState( id, STOPPED );
    }
}
