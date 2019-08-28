/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.stream.Collectors;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.dbms.OperatorState.STOPPED;
import static org.neo4j.kernel.database.DatabaseIdRepository.SYSTEM_DATABASE_ID;

/**
 * Operator responsible for transitioning all non-DROPPED databases to a STOPPED state.
 * The system database is reconciled only after all other databases have successfully stopped.
 */
class ShutdownOperator extends DbmsOperator
{
    private final DatabaseManager<?> databaseManager;

    ShutdownOperator( DatabaseManager<?> databaseManager )
    {
        this.databaseManager = databaseManager;
    }

    void stopAll()
    {
        desired.clear();
        var desireAllStopped = databaseManager.registeredDatabases().keySet().stream()
                .filter( e -> !e.equals( SYSTEM_DATABASE_ID ) )
                .collect( Collectors.toMap( DatabaseId::name, this::stoppedState ) );
        desired.putAll( desireAllStopped );
        trigger( ReconcilerRequest.force() ).awaitAll();

        desired.put( SYSTEM_DATABASE_ID.name(), stoppedState( SYSTEM_DATABASE_ID ) );
        trigger( ReconcilerRequest.force() ).await( SYSTEM_DATABASE_ID );
    }

    private DatabaseState stoppedState( DatabaseId id )
    {
        return new DatabaseState( id, STOPPED );
    }

}
