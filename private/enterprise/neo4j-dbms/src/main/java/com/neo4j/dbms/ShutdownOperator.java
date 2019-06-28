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

import static com.neo4j.dbms.OperatorState.STOPPED;

public class ShutdownOperator implements DbmsOperator
{
    private final DatabaseManager<?> databaseManager;
    private volatile Map<DatabaseId,OperatorState> desired;
    private OperatorConnector connector;

    ShutdownOperator( DatabaseManager<?> databaseManager )
    {
        this.databaseManager = databaseManager;
        this.desired = Collections.emptyMap();
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
        //TODO: Stop system db separately, afterwards
        this.desired = databaseManager.registeredDatabases().entrySet().stream()
                .collect( Collectors.toMap( Map.Entry::getKey, ignored -> STOPPED ) );
        trigger().awaitAll();
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
