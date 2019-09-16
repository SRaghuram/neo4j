/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.storageengine.api.StoreId;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toSet;

public class ClusterSystemGraphDbmsModel extends EnterpriseSystemGraphDbmsModel
{
    static final String INITIAL_MEMBERS = "initial_members";
    static final String STORE_CREATION_TIME = "store_creation_time";
    static final String STORE_RANDOM_ID = "store_random_id";
    static final String STORE_VERSION = "store_version";

    public ClusterSystemGraphDbmsModel( Supplier<GraphDatabaseService> systemDatabase )
    {
        super( systemDatabase );
    }

    /**
     * Initial members are not written for databases created during cluster formation.
     */
    public Set<UUID> getInitialMembers( DatabaseId databaseId )
    {
        try ( var tx = systemDatabase.get().beginTx() )
        {
            Node node = tx.findNode( DATABASE_LABEL, DATABASE_UUID_PROPERTY, databaseId.uuid().toString() );
            String[] initialMembers = (String[]) node.getProperty( INITIAL_MEMBERS );
            return Arrays.stream( initialMembers )
                         .map( UUID::fromString )
                         .collect( toSet() );
        }
        catch ( NotFoundException e )
        {
            return emptySet();
        }
    }

    public StoreId getStoreId( DatabaseId databaseId )
    {
        try ( var tx = systemDatabase.get().beginTx() )
        {
            Node node = tx.findNode( DATABASE_LABEL, DATABASE_UUID_PROPERTY, databaseId.uuid().toString() );
            long creationTime = (long) node.getProperty( STORE_CREATION_TIME );
            long randomId = (long) node.getProperty( STORE_RANDOM_ID );
            String storeVersion = (String) node.getProperty( STORE_VERSION );
            return new StoreId( creationTime, randomId, MetaDataStore.versionStringToLong( storeVersion ) );
        }
    }
}
