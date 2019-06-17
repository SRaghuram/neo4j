/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.dbms.database.DatabaseManager;

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
        var desireAllStopped = databaseManager.registeredDatabases().entrySet().stream()
                .filter( e -> !e.getKey().equals( SYSTEM_DATABASE_ID ) )
                .collect( Collectors.toMap( Map.Entry::getKey, ignored -> STOPPED ) );
        desired.putAll( desireAllStopped );
        trigger( true ).awaitAll();

        desired.put( SYSTEM_DATABASE_ID, STOPPED );
        trigger( true ).await( SYSTEM_DATABASE_ID );
    }
}
