/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

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
        var id = databaseIdRepository.get( databaseName );
        desired.put( id, DROPPED );
        trigger( true ).await( id );
    }

    public void startDatabase( String databaseName )
    {
        var id = databaseIdRepository.get( databaseName );
        desired.put( id, STARTED );
        trigger( true ).await( id );
    }

    public void stopDatabase( String databaseName )
    {
        var id = databaseIdRepository.get( databaseName );
        desired.put( id, STOPPED );
        trigger( true ).await( id );
    }

}
