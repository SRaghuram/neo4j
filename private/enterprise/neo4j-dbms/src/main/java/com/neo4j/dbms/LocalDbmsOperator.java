/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;

import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;

/**
 * Database operator for local administrative overrides of system-wide operator.
 *
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
        desired.put( databaseName, new EnterpriseDatabaseState( id, DROPPED ) );
        trigger( ReconcilerRequest.priorityTarget( id ).build() ).await( id );
    }

    public void startDatabase( String databaseName )
    {
        var id = databaseId( databaseName );
        desired.put( databaseName, new EnterpriseDatabaseState( id, STARTED ) );
        trigger( ReconcilerRequest.priorityTarget( id ).build() ).await( id );
    }

    public void stopDatabase( String databaseName )
    {
        var id = databaseId( databaseName );
        desired.put( databaseName, new EnterpriseDatabaseState( id, STOPPED ) );
        trigger( ReconcilerRequest.priorityTarget( id ).build() ).await( id );
    }

    private NamedDatabaseId databaseId( String databaseName )
    {
        return databaseIdRepository.getByName( databaseName )
                .orElseThrow( () -> new DatabaseNotFoundException( "Cannot find database: " + databaseName ) );
    }
}
