/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Map;
import java.util.stream.Collectors;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;

import static com.neo4j.dbms.OperatorState.STOPPED;

/**
 * Operator responsible for transitioning all non-DROPPED databases to a STOPPED state.
 * The system database is reconciled only after all other databases have successfully stopped.
 */
class ShutdownOperator extends DbmsOperator
{
    private final DatabaseManager<?> databaseManager;
    private final DatabaseId systemDatabaseId;

    ShutdownOperator( DatabaseManager<?> databaseManager, DatabaseIdRepository databaseIdRepository )
    {
        this.databaseManager = databaseManager;
        this.systemDatabaseId = databaseIdRepository.systemDatabase();
    }

    void stopAll()
    {
        desired.clear();
        var desireAllStopped = databaseManager.registeredDatabases().entrySet().stream()
                .filter( e -> !e.getKey().equals( systemDatabaseId ) )
                .collect( Collectors.toMap( Map.Entry::getKey, ignored -> STOPPED ) );
        desired.putAll( desireAllStopped );
        trigger( true ).awaitAll();

        desired.put( systemDatabaseId, STOPPED );
        trigger( true ).await( systemDatabaseId );
    }
}
