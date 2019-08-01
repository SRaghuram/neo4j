/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.neo4j.kernel.database.DatabaseId;

public interface TransactionEventService
{
    void registerHandler( DatabaseId databaseId, TransactionCommitHandler handler );

    TransactionCommitNotifier getCommitNotifier( DatabaseId databaseId );

    @FunctionalInterface
    interface TransactionCommitHandler
    {
        void transactionCommitted( long txId );
    }

    @FunctionalInterface
    interface TransactionCommitNotifier
    {
        void transactionCommitted( long txId );
    }
}
