/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static com.neo4j.dbms.ClusterSystemGraphDbmsModel.INITIAL_SERVERS;
import static com.neo4j.dbms.ClusterSystemGraphDbmsModel.STORE_CREATION_TIME;
import static com.neo4j.dbms.ClusterSystemGraphDbmsModel.STORE_RANDOM_ID;
import static com.neo4j.dbms.ClusterSystemGraphDbmsModel.STORE_VERSION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_NAME_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_STATUS_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_UUID_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DELETED_DATABASE_LABEL;
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
        var databaseId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var deletedDatabaseId = DatabaseIdFactory.from( "foo", UUID.randomUUID() );
        var expectedMembers = Set.of( UUID.randomUUID(), UUID.randomUUID() );

        var storeId1 = new StoreId( 1, 2, MetaDataStore.versionStringToLong( LATEST_STORE_VERSION ) );
        var storeId2 = new StoreId( 2, 7, MetaDataStore.versionStringToLong( LATEST_STORE_VERSION ) );

        try ( var tx = db.beginTx() )
        {
            makeDatabaseNodeForCluster( tx, databaseId, expectedMembers, storeId1, false );
            makeDatabaseNodeForCluster( tx, deletedDatabaseId, expectedMembers, storeId2, true );
            tx.commit();
        }
        // when
        var initialServers = dbmsModel.getInitialServers( databaseId );
        var deletedInitialServers = dbmsModel.getInitialServers( deletedDatabaseId );
        var storeId = dbmsModel.getStoreId( databaseId );
        var deletedStoreId = dbmsModel.getStoreId( deletedDatabaseId );

        // then
        assertFalse( initialServers.isEmpty() );
        assertFalse( deletedInitialServers.isEmpty() );
        assertEquals( expectedMembers, initialServers );
        assertEquals( expectedMembers, deletedInitialServers );
        assertEquals( storeId1, storeId );
        assertEquals( storeId2, deletedStoreId );
    }

    @Test
    void shouldThrowIfDatabaseNodeDoesNotExist()
    {
        // given
        var nonExistentDatabaseId = DatabaseIdFactory.from( "bar", UUID.randomUUID() );

        // when/then
        assertThrows( IllegalStateException.class, () -> dbmsModel.getStoreId( nonExistentDatabaseId ) );
        assertThrows( IllegalStateException.class, () -> dbmsModel.getInitialServers( nonExistentDatabaseId ) );
    }

    private void makeDatabaseNodeForCluster( Transaction tx, NamedDatabaseId namedDatabaseId, Set<UUID> initialMembers, StoreId storeId, boolean deleted )
    {
        var label = deleted ? DELETED_DATABASE_LABEL : DATABASE_LABEL;
        Node node = tx.createNode( label );
        node.setProperty( DATABASE_NAME_PROPERTY, namedDatabaseId.name() );
        node.setProperty( DATABASE_STATUS_PROPERTY, "online" );
        node.setProperty( DATABASE_UUID_PROPERTY, namedDatabaseId.databaseId().uuid().toString() );

        node.setProperty( INITIAL_SERVERS, initialMembers.stream().map( UUID::toString ).toArray( String[]::new ) );

        node.setProperty( STORE_CREATION_TIME, storeId.getCreationTime() );
        node.setProperty( STORE_RANDOM_ID, storeId.getRandomId() );
        node.setProperty( STORE_VERSION, MetaDataStore.versionLongToString( storeId.getStoreVersion() ) );
    }
}
