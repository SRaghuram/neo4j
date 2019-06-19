/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.neo4j.kernel.database.DatabaseId;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class SystemDatabaseOnlyTransactionEventService implements TransactionEventService
{
    private TransactionCommitHandler handler;

    public void registerHandler( DatabaseId databaseId, TransactionCommitHandler handler )
    {
        if ( isNotSystemDatabase( databaseId ) )
        {
            return;
        }

        if ( this.handler != null )
        {
            throw new IllegalStateException( "Only one handler supported" );
        }

        this.handler = handler;
    }

    public TransactionCommitNotifier getCommitNotifier( DatabaseId databaseId )
    {
        if ( isNotSystemDatabase( databaseId ) )
        {
            return ignored -> {};
        }

        return txId ->
        {
            if ( handler != null )
            {
                handler.transactionCommitted( txId );
            }
        };
    }

    private boolean isNotSystemDatabase( DatabaseId databaseId )
    {
        return !databaseId.name().equals( SYSTEM_DATABASE_NAME );
    }
}
