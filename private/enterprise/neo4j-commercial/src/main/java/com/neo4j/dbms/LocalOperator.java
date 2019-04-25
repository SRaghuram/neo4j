/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.dbms.OperatorState.DROPPED;
import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;

/**
 * Database operator for local administrative overrides of system-wide operator.
 */
public class LocalOperator implements Operator
{
    private final Map<DatabaseId,OperatorState> desired = new ConcurrentHashMap<>();
    private final OperatorConnector connector;

    public LocalOperator( OperatorConnector connector )
    {
        this.connector = connector;
        connector.register( this );
    }

    public void createDatabase( String databaseName )
    {
        desired.put( new DatabaseId( databaseName ), STOPPED );
        connector.trigger();
    }

    public void dropDatabase( String databaseName )
    {
        desired.put( new DatabaseId( databaseName ), DROPPED );
        connector.trigger();
    }

    public void startDatabase( String databaseName )
    {
        desired.put( new DatabaseId( databaseName ), STARTED );
        connector.trigger();
    }

    public void stopDatabase( String databaseName )
    {
        desired.put( new DatabaseId( databaseName ), STOPPED );
        connector.trigger();
    }

    @Override
    public Map<DatabaseId,OperatorState> getDesired()
    {
        return new HashMap<>( desired );
    }
}
