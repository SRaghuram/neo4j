/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;

import static com.neo4j.dbms.OperatorState.DROPPED;
import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;

/**
 * Database operator for local administrative overrides of system-wide operator.
 */
public final class LocalDbmsOperator extends DbmsOperator
{
    private final DatabaseIdRepository databaseIdRepository;

    LocalDbmsOperator( DatabaseIdRepository databaseIdRepository )
    {
        this.databaseIdRepository = databaseIdRepository;
    }

    public void dropDatabase( String databaseName )
    {
        var id = databaseId( databaseName );
        desired.put( databaseName, new DatabaseState( id, DROPPED ) );
        trigger( ReconcilerRequest.force() ).await( databaseName );
    }

    public void startDatabase( String databaseName )
    {
        var id = databaseId( databaseName );
        desired.put( databaseName, new DatabaseState( id, STARTED ) );
        trigger( ReconcilerRequest.force() ).await( databaseName );
    }

    public void stopDatabase( String databaseName )
    {
        var id = databaseId( databaseName );
        desired.put( databaseName, new DatabaseState( id, STOPPED ) );
        trigger( ReconcilerRequest.force() ).await( databaseName );
    }

    private DatabaseId databaseId( String databaseName )
    {
        return databaseIdRepository.getByName( databaseName )
                .orElseThrow( () -> new DatabaseNotFoundException( "Cannot find database: " + databaseName ) );
    }
}
