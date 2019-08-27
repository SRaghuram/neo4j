/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise;

import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventListener;
import com.neo4j.kernel.enterprise.api.security.CommercialAuthManager;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;

class AuthCacheClearingDatabaseEventListener implements ReplicatedDatabaseEventListener, TransactionEventListener<Object>
{
    private CommercialAuthManager authManager;

    AuthCacheClearingDatabaseEventListener( CommercialAuthManager authManager )
    {
        this.authManager = authManager;
    }

    @Override
    public void transactionCommitted( long txId )
    {
        authManager.clearAuthCache();
    }

    @Override
    public void storeReplaced( long txId )
    {
        authManager.clearAuthCache();
    }

    @Override
    public void afterCommit( TransactionData data, Object state, GraphDatabaseService databaseService )
    {
        authManager.clearAuthCache();
    }

    @Override
    public Object beforeCommit( TransactionData data, GraphDatabaseService databaseService )
    {
        return null;
    }

    @Override
    public void afterRollback( TransactionData data, Object state, GraphDatabaseService databaseService )
    {
    }
}
