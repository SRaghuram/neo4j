/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.UUID;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static com.neo4j.dbms.ClusterSystemGraphDbmsModel.INITIAL_MEMBERS;
import static com.neo4j.dbms.ClusterSystemGraphDbmsModel.STORE_CREATION_TIME;
import static com.neo4j.dbms.ClusterSystemGraphDbmsModel.STORE_RANDOM_ID;
import static com.neo4j.dbms.ClusterSystemGraphDbmsModel.STORE_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_NAME_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_STATUS_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_UUID_PROPERTY;
import static org.neo4j.kernel.impl.store.format.standard.Standard.LATEST_STORE_VERSION;

@ImpermanentDbmsExtension
class ClusterSystemGraphDbmsModelTest
{
    @Inject
    private DatabaseManagementService managementService;

    @Inject
    private GraphDatabaseService db;

    private ClusterSystemGraphDbmsModel dbmsModel;

    @BeforeEach
    void before()
    {
        dbmsModel = new ClusterSystemGraphDbmsModel( () -> db );
    }

    @Test
    void shouldReturnInitialMembersAndStoreParameters()
    {
        // given
        DatabaseId databaseId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        Set<UUID> expectedMembers = Set.of( UUID.randomUUID(), UUID.randomUUID() );

        StoreId expectedStoreId = new StoreId( 1, 2, MetaDataStore.versionStringToLong( LATEST_STORE_VERSION ) );

        try ( var tx = db.beginTx() )
        {
            makeDatabaseNodeForCluster( tx, databaseId, expectedMembers, expectedStoreId );
            tx.commit();
        }

        // when
        Set<UUID> initialMembers = dbmsModel.getInitialMembers( databaseId );
        StoreId storeId = dbmsModel.getStoreId( databaseId );

        // then
        assertFalse( initialMembers.isEmpty() );
        assertEquals( expectedMembers, initialMembers );
        assertEquals( expectedStoreId, storeId );
    }

    private void makeDatabaseNodeForCluster( Transaction tx, DatabaseId databaseId, Set<UUID> initialMembers, StoreId storeId )
    {
        Node node = tx.createNode( DATABASE_LABEL );
        node.setProperty( DATABASE_NAME_PROPERTY, databaseId.name() );
        node.setProperty( DATABASE_STATUS_PROPERTY, "online" );
        node.setProperty( DATABASE_UUID_PROPERTY, databaseId.uuid().toString() );

        node.setProperty( INITIAL_MEMBERS, initialMembers.stream().map( UUID::toString ).toArray( String[]::new ) );

        node.setProperty( STORE_CREATION_TIME, storeId.getCreationTime() );
        node.setProperty( STORE_RANDOM_ID, storeId.getRandomId() );
        node.setProperty( STORE_VERSION, MetaDataStore.versionLongToString( storeId.getStoreVersion() ) );
    }
}
