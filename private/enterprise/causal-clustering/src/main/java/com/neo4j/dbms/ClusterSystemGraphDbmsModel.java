/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.storageengine.api.StoreId;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

public class ClusterSystemGraphDbmsModel extends EnterpriseSystemGraphDbmsModel
{
    static final String INITIAL_SERVERS = "initial_members"; // TODO: migrate name of field?
    static final String STORE_CREATION_TIME = "store_creation_time";
    static final String STORE_RANDOM_ID = "store_random_id";
    static final String STORE_VERSION = "store_version";

    public ClusterSystemGraphDbmsModel( Supplier<GraphDatabaseService> systemDatabase )
    {
        super( systemDatabase );
    }

    /**
     * Initial servers are not written for databases created during cluster formation.
     */
    public Set<UUID> getInitialServers( NamedDatabaseId namedDatabaseId )
    {
        try ( var tx = systemDatabase.get().beginTx() )
        {
            var node = databaseNode( namedDatabaseId, tx );

            String[] initialServers = (String[]) node.getProperty( INITIAL_SERVERS );
            return Arrays.stream( initialServers )
                         .map( UUID::fromString )
                         .collect( toSet() );
        }
        catch ( NotFoundException e )
        {
            return emptySet();
        }
    }

    public StoreId getStoreId( NamedDatabaseId namedDatabaseId )
    {
        try ( var tx = systemDatabase.get().beginTx() )
        {
            var node = databaseNode( namedDatabaseId, tx );
            long creationTime = (long) node.getProperty( STORE_CREATION_TIME );
            long randomId = (long) node.getProperty( STORE_RANDOM_ID );
            String storeVersion = (String) node.getProperty( STORE_VERSION );
            return new StoreId( creationTime, randomId, MetaDataStore.versionStringToLong( storeVersion ) );
        }
    }

    private Node databaseNode( NamedDatabaseId databaseId, Transaction tx )
    {
        return Optional.ofNullable( tx.findNode( DATABASE_LABEL, DATABASE_UUID_PROPERTY, databaseId.databaseId().uuid().toString() ) )
                .or( () -> Optional.ofNullable( tx.findNode( DELETED_DATABASE_LABEL, DATABASE_UUID_PROPERTY, databaseId.databaseId().uuid().toString() ) ) )
                .orElseThrow( () -> new IllegalStateException( "System database must contain a node for " + databaseId ) );
    }

    public static void clearClusterProperties( GraphDatabaseService system )
    {
        try ( Transaction tx = system.beginTx() )
        {
            try ( ResourceIterator<Node> databaseItr = tx.findNodes( DATABASE_LABEL ) )
            {
                while ( databaseItr.hasNext() )
                {
                    Node database = databaseItr.next();
                    database.removeProperty( INITIAL_SERVERS );
                    database.removeProperty( STORE_CREATION_TIME );
                    database.removeProperty( STORE_RANDOM_ID );
                    database.removeProperty( STORE_VERSION );
                }
            }
            tx.commit();
        }
    }
}
