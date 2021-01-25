/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.neo4j.kernel.database.NamedDatabaseId;

public interface ReplicatedDatabaseEventService
{
    void registerListener( NamedDatabaseId namedDatabaseId, ReplicatedDatabaseEventListener listener );

    void unregisterListener( NamedDatabaseId namedDatabaseId, ReplicatedDatabaseEventListener listener );

    ReplicatedDatabaseEventDispatch getDatabaseEventDispatch( NamedDatabaseId namedDatabaseId );

    interface ReplicatedDatabaseEventListener
    {
        /**
         * Called when a transaction has been committed.
         * There is no guarantee on the order in which this event fires, meaning that txId might not increase monotonically.
         */
        void transactionCommitted( long txId );

        /**
         * Called when the store has been replaced due to a store copy.
         */
        void storeReplaced( long txId );
    }

    interface ReplicatedDatabaseEventDispatch
    {
        void fireTransactionCommitted( long txId );

        void fireStoreReplaced( long txId );
    }

    ReplicatedDatabaseEventDispatch NO_EVENT_DISPATCH = new ReplicatedDatabaseEventDispatch()
    {
        @Override
        public void fireTransactionCommitted( long txId )
        {
        }

        @Override
        public void fireStoreReplaced( long txId )
        {
        }
    };
}
