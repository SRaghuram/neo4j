/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.UUID;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static com.neo4j.dbms.SystemGraphDbmsModel.DatabaseState.DELETED;
import static com.neo4j.dbms.SystemGraphDbmsModel.DatabaseState.OFFLINE;
import static com.neo4j.dbms.SystemGraphDbmsModel.DatabaseState.ONLINE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.SystemGraphInitializer.DATABASE_LABEL;
import static org.neo4j.dbms.database.SystemGraphInitializer.DELETED_DATABASE_LABEL;

@ImpermanentDbmsExtension
class SystemGraphDbmsModelTest
{
    @Inject
    private DatabaseManagementService managementService;
    @Inject
    private GraphDatabaseService db;
    private TestDatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
    private Collection<DatabaseId> updatedDatabases = new ArrayList<>();
    private SystemGraphDbmsModel dbmsModel;

    @BeforeEach
    void before()
    {
        dbmsModel = new SystemGraphDbmsModel();
        dbmsModel.setSystemDatabase( db );

        managementService.registerTransactionEventListener( DEFAULT_DATABASE_NAME, new TransactionEventListenerAdapter<>()
        {
            @Override
            public void afterCommit( TransactionData txData, Object state, GraphDatabaseService databaseService )
            {
                updatedDatabases.addAll( dbmsModel.updatedDatabases( txData ) );
            }
        } );
    }

    @Test
    void shouldDetectUpdatedDatabases()
    {
        // when
        HashMap<DatabaseId,SystemGraphDbmsModel.DatabaseState> expectedCreated = new HashMap<>();
        try ( var tx = db.beginTx() )
        {
            makeDatabaseNode( "A", true, expectedCreated );
            makeDatabaseNode( "B", true, expectedCreated );
            makeDatabaseNode( "C", false, expectedCreated );
            tx.success();
        }

        // then
        assertThat( updatedDatabases, containsInAnyOrder( expectedCreated.keySet().toArray() ) );

        // given
        updatedDatabases.clear();

        // when
        HashMap<DatabaseId,SystemGraphDbmsModel.DatabaseState> expectedDeleted = new HashMap<>();
        try ( var tx = db.beginTx() )
        {
            makeDeletedDatabaseNode( "D", expectedDeleted );
            makeDeletedDatabaseNode( "E", expectedDeleted );
            makeDeletedDatabaseNode( "F", expectedDeleted );
            tx.success();
        }

        // then
        assertThat( updatedDatabases, containsInAnyOrder( expectedDeleted.keySet().toArray() ) );
    }

    @Test
    void shouldReturnDatabaseStates()
    {

        // when
        HashMap<DatabaseId,SystemGraphDbmsModel.DatabaseState> expected = new HashMap<>();
        try ( var tx = db.beginTx() )
        {
            makeDatabaseNode( "A", true, expected );
            makeDatabaseNode( "B", false, expected );
            makeDeletedDatabaseNode( "C", expected );
            tx.success();
        }

        try ( var tx = db.beginTx() )
        {
            makeDatabaseNode( "D", true, expected );
            makeDeletedDatabaseNode( "E", expected );
            makeDeletedDatabaseNode( "F", expected );
            tx.success();
        }

        // then
        assertEquals( expected, dbmsModel.getDatabaseStates() );
    }

    private void makeDatabaseNode( String databaseName, boolean online, HashMap<DatabaseId,SystemGraphDbmsModel.DatabaseState> expected )
    {
        UUID uuid = UUID.randomUUID();
        Node node = db.createNode( DATABASE_LABEL );
        node.setProperty( "name", databaseName );
        node.setProperty( "status", online ? "online" : "offline" );
        node.setProperty( "uuid", uuid.toString() );
        expected.put( DatabaseIdFactory.from( databaseName, uuid ), online ? ONLINE : OFFLINE );
    }

    private void makeDeletedDatabaseNode( String databaseName, HashMap<DatabaseId,SystemGraphDbmsModel.DatabaseState> expected )
    {
        UUID uuid = UUID.randomUUID();
        Node node = db.createNode( DELETED_DATABASE_LABEL );
        node.setProperty( "name", databaseName );
        node.setProperty( "uuid", uuid.toString() );
        expected.put( DatabaseIdFactory.from( databaseName, uuid ), DELETED );
    }
}
