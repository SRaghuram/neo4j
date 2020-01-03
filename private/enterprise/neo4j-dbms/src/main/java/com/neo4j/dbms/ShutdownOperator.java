/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.stream.Collectors;

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

    ShutdownOperator( DatabaseManager<?> databaseManager )
    {
        this.databaseManager = databaseManager;
    }

    void stopAll()
    {
        desired.clear();
        var desireAllStopped = databaseManager.registeredDatabases().keySet().stream()
                .filter( e -> !e.equals( NAMED_SYSTEM_DATABASE_ID ) )
                .collect( Collectors.toMap( NamedDatabaseId::name, this::stoppedState ) );
        desired.putAll( desireAllStopped );
        trigger( ReconcilerRequest.force() ).awaitAll();

        desired.put( NAMED_SYSTEM_DATABASE_ID.name(), stoppedState( NAMED_SYSTEM_DATABASE_ID ) );
        trigger( ReconcilerRequest.force() ).await( NAMED_SYSTEM_DATABASE_ID );
    }

    private EnterpriseDatabaseState stoppedState( NamedDatabaseId id )
    {
        return new EnterpriseDatabaseState( id, STOPPED );
    }

}
