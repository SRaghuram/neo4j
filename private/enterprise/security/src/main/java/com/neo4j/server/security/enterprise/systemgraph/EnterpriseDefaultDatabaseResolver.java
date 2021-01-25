/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.systemgraph;

import com.github.benmanes.caffeine.cache.Cache;
import com.neo4j.dbms.ReplicatedDatabaseEventService.ReplicatedDatabaseEventListener;

import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.cypher.internal.cache.CaffeineCacheFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.server.security.systemgraph.CommunityDefaultDatabaseResolver;

public class EnterpriseDefaultDatabaseResolver extends CommunityDefaultDatabaseResolver implements ReplicatedDatabaseEventListener
{

    private static final int DEFAULT_DATABASE_CACHE_SIZE = 1000;
    private final Supplier<GraphDatabaseService> systemDbSupplier;
    private final Cache<String,String> cache;
    private GraphDatabaseService systemDb;

    public EnterpriseDefaultDatabaseResolver( Config config, CaffeineCacheFactory caffeineCacheFactory, Supplier<GraphDatabaseService> systemDbSupplier )
    {
        super( config, systemDbSupplier );
        this.systemDbSupplier = systemDbSupplier;
        this.cache = caffeineCacheFactory.createCache( DEFAULT_DATABASE_CACHE_SIZE );
    }

    @Override
    public String defaultDatabase( String username )
    {
        if ( username == null || username.isBlank() )
        {
            return super.defaultDatabase( username );
        }
        else
        {
            return cache.get( username, u ->
            {
                String defaultDatabase = "";
                try ( Transaction tx = getSystemDb().beginTx() )
                {
                    Node userNode = tx.findNode( Label.label( "User" ), "name", u );
                    if ( userNode != null && userNode.hasProperty( "defaultDatabase" ) )
                    {
                        defaultDatabase = (String) userNode.getProperty( "defaultDatabase" );
                    }
                    else
                    {
                        defaultDatabase = super.defaultDatabase( u );
                    }
                    tx.commit();
                }
                catch ( NotFoundException n )
                {
                    defaultDatabase = super.defaultDatabase( u );
                }
                return defaultDatabase;
            } );
        }
    }

    @Override
    public void clearCache()
    {
        cache.invalidateAll();
        super.clearCache();
    }

    private GraphDatabaseService getSystemDb()
    {
        if ( systemDb == null )
        {
            systemDb = systemDbSupplier.get();
        }
        return systemDb;
    }

    @Override
    public void transactionCommitted( long txId )
    {
        clearCache();
    }

    @Override
    public void storeReplaced( long txId )
    {
        clearCache();
    }

    @Override
    public void afterCommit( TransactionData data, Object state, GraphDatabaseService databaseService )
    {
        clearCache();
    }
}
