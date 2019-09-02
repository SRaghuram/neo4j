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
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static com.neo4j.dbms.OperatorState.DROPPED;
import static com.neo4j.dbms.OperatorState.STARTED;
import static com.neo4j.dbms.OperatorState.STOPPED;
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
    private Collection<String> updatedDatabases = new ArrayList<>();
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
        HashMap<String,DatabaseState> expectedCreated = new HashMap<>();
        try ( var tx = db.beginTx() )
        {
            makeDatabaseNode( tx, "A", true, expectedCreated );
            makeDatabaseNode( tx, "B", true, expectedCreated );
            makeDatabaseNode( tx, "C", false, expectedCreated );
            tx.commit();
        }

        // then
        assertThat( updatedDatabases, containsInAnyOrder( expectedCreated.keySet().toArray() ) );

        // given
        updatedDatabases.clear();

        // when
        HashMap<String,DatabaseState> expectedDeleted = new HashMap<>();
        try ( var tx = db.beginTx() )
        {
            makeDeletedDatabaseNode( tx, "D", expectedDeleted );
            makeDeletedDatabaseNode( tx, "E", expectedDeleted );
            makeDeletedDatabaseNode( tx, "F", expectedDeleted );
            tx.commit();
        }

        // then
        assertThat( updatedDatabases, containsInAnyOrder( expectedDeleted.keySet().toArray() ) );
    }

    @Test
    void shouldReturnDatabaseStates()
    {

        // when
        HashMap<String,DatabaseState> expected = new HashMap<>();
        try ( var tx = db.beginTx() )
        {
            makeDatabaseNode( tx, "A", true, expected );
            makeDatabaseNode( tx, "B", false, expected );
            makeDeletedDatabaseNode( tx,"C", expected );
            tx.commit();
        }

        try ( var tx = db.beginTx() )
        {
            makeDatabaseNode( tx, "D", true, expected );
            makeDeletedDatabaseNode( tx, "E", expected );
            makeDeletedDatabaseNode( tx, "F", expected );
            tx.commit();
        }

        // then
        assertEquals( expected, dbmsModel.getDatabaseStates() );
    }

    private void makeDatabaseNode( Transaction tx, String databaseName, boolean online, HashMap<String,DatabaseState> expected )
    {
        UUID uuid = UUID.randomUUID();
        Node node = tx.createNode( DATABASE_LABEL );
        node.setProperty( "name", databaseName );
        node.setProperty( "status", online ? "online" : "offline" );
        node.setProperty( "uuid", uuid.toString() );
        var id = DatabaseIdFactory.from( databaseName, uuid );
        expected.put( databaseName, online ? new DatabaseState( id, STARTED ) : new DatabaseState( id, STOPPED ) );
    }

    private void makeDeletedDatabaseNode( Transaction tx, String databaseName, HashMap<String,DatabaseState> expected )
    {
        UUID uuid = UUID.randomUUID();
        Node node = tx.createNode( DELETED_DATABASE_LABEL );
        node.setProperty( "name", databaseName );
        node.setProperty( "uuid", uuid.toString() );
        var id = DatabaseIdFactory.from( databaseName, uuid );
        expected.put( databaseName , new DatabaseState( id, DROPPED ) );
    }

}
