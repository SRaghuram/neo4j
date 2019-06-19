/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;

import static com.neo4j.dbms.OperatorState.DROPPED;
import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;

/**
 * Database operator for local administrative overrides of system-wide operator.
 */
public class LocalDbmsOperator implements DbmsOperator
{
    private final Map<DatabaseId,OperatorState> desired = new ConcurrentHashMap<>();
    private final DatabaseIdRepository databaseIdRepository;
    private OperatorConnector connector;

    public LocalDbmsOperator( DatabaseIdRepository databaseIdRepository )
    {
        this.databaseIdRepository = databaseIdRepository;
    }

    @Override
    public void connect( OperatorConnector connector )
    {
        Objects.requireNonNull( connector );
        this.connector = connector;
        connector.register( this );
    }

    public void dropDatabase( String databaseName )
    {
        var id = databaseIdRepository.get( databaseName );
        desired.put( id, DROPPED );
        trigger().await( id );
    }

    public void startDatabase( String databaseName )
    {
        var id = databaseIdRepository.get( databaseName );
        desired.put( id, STARTED );
        trigger().await( id );
    }

    public void stopDatabase( String databaseName )
    {
        var id = databaseIdRepository.get( databaseName );
        desired.put( id, STOPPED );
        trigger().await( id );
    }

    @Override
    public Map<DatabaseId,OperatorState> getDesired()
    {
        return new HashMap<>( desired );
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
