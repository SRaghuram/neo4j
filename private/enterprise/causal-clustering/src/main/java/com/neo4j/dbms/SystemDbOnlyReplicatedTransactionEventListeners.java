/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.neo4j.kernel.database.DatabaseId;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public class SystemDbOnlyReplicatedTransactionEventListeners implements ReplicatedTransactionEventListeners
{
    private List<TransactionCommitListener> listeners = new CopyOnWriteArrayList<>();

    public void registerListener( DatabaseId databaseId, TransactionCommitListener listener )
    {
        if ( isNotSystemDatabase( databaseId ) )
        {
            return;
        }

        this.listeners.add( listener );
    }

    public void unregisterListener( DatabaseId databaseId, TransactionCommitListener listener )
    {
        if ( isNotSystemDatabase( databaseId ) )
        {
            return;
        }

        this.listeners.remove( listener );
    }

    public TransactionCommitNotifier getCommitNotifier( DatabaseId databaseId )
    {
        if ( isNotSystemDatabase( databaseId ) )
        {
            return ignored -> {};
        }

        return txId -> listeners.forEach( h -> h.transactionCommitted( txId ) );
    }

    private boolean isNotSystemDatabase( DatabaseId databaseId )
    {
        return !databaseId.name().equals( SYSTEM_DATABASE_NAME );
    }
}
