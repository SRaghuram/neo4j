/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise;

import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventListener;
import com.neo4j.kernel.enterprise.api.security.EnterpriseAuthManager;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;

class AuthCacheClearingDatabaseEventListener extends TransactionEventListenerAdapter<Object> implements ReplicatedDatabaseEventListener
{
    private final EnterpriseAuthManager authManager;

    AuthCacheClearingDatabaseEventListener( EnterpriseAuthManager authManager )
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
}
