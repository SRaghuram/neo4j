/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;

import static com.neo4j.dbms.OperatorState.STOPPED;

/**
 * Operator responsible for transitioning all non-DROPPED databases to a STOPPED state.
 * The system database is reconciled only after all other databases have successfully stopped.
 */
public class ShutdownOperator implements DbmsOperator
{
    private final DatabaseManager<?> databaseManager;
    private final DatabaseId systemDatabaseId;
    private volatile Map<DatabaseId,OperatorState> desired;
    private OperatorConnector connector;

    ShutdownOperator( DatabaseManager<?> databaseManager, DatabaseIdRepository databaseIdRepository )
    {
        this.databaseManager = databaseManager;
        this.desired = Collections.emptyMap();
        this.systemDatabaseId = databaseIdRepository.systemDatabase();
    }

    @Override
    public void connect( OperatorConnector connector )
    {
        Objects.requireNonNull( connector );
        this.connector = connector;
        connector.register( this );
    }

    @Override
    public Map<DatabaseId,OperatorState> getDesired()
    {
        return desired;
    }

    void stopAll()
    {
        desired = databaseManager.registeredDatabases().entrySet().stream()
                .filter( e -> !e.getKey().equals( systemDatabaseId ) )
                .collect( Collectors.toMap( Map.Entry::getKey, ignored -> STOPPED ) );
        trigger().awaitAll();

        desired.put( systemDatabaseId, STOPPED );
        trigger().await( systemDatabaseId );
    }

    private Reconciliation trigger()
    {
        if ( connector == null )
        {
            return Reconciliation.EMPTY;
        }
        return connector.trigger( true );
    }
}
