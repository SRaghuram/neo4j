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
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListenerAdapter;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.test.extension.ImpermanentDbmsExtension;
import org.neo4j.test.extension.Inject;

import static com.neo4j.dbms.EnterpriseOperatorState.DROPPED;
import static com.neo4j.dbms.EnterpriseOperatorState.STARTED;
import static com.neo4j.dbms.EnterpriseOperatorState.STOPPED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_LABEL;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_NAME_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_STATUS_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DATABASE_UUID_PROPERTY;
import static org.neo4j.dbms.database.SystemGraphDbmsModel.DELETED_DATABASE_LABEL;

@ImpermanentDbmsExtension
class EnterpriseSystemGraphDbmsModelTest
{
    @Inject
    private DatabaseManagementService managementService;

    @Inject
    private GraphDatabaseService db;
    private Collection<NamedDatabaseId> updatedDatabases = new ArrayList<>();
    private EnterpriseSystemGraphDbmsModel dbmsModel;

    @BeforeEach
    void before()
    {
        dbmsModel = new EnterpriseSystemGraphDbmsModel( () -> db );

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
        HashMap<NamedDatabaseId,EnterpriseDatabaseState> expectedCreated = new HashMap<>();
        try ( var tx = db.beginTx() )
        {
            makeDatabaseNode( tx, "A", true, expectedCreated );
            makeDatabaseNode( tx, "B", true, expectedCreated );
            makeDatabaseNode( tx, "C", false, expectedCreated );
            tx.commit();
        }

        // then
        assertThat( updatedDatabases ).containsAll( expectedCreated.keySet() );

        // given
        updatedDatabases.clear();

        // when
        HashMap<NamedDatabaseId,EnterpriseDatabaseState> expectedDeleted = new HashMap<>();
        try ( var tx = db.beginTx() )
        {
            makeDeletedDatabaseNode( tx, "D", expectedDeleted );
            makeDeletedDatabaseNode( tx, "E", expectedDeleted );
            makeDeletedDatabaseNode( tx, "F", expectedDeleted );
            tx.commit();
        }

        // then
        assertThat( updatedDatabases ).containsAll( expectedDeleted.keySet() );
    }

    @Test
    void shouldReturnDatabaseStates()
    {
        // when
        HashMap<NamedDatabaseId,EnterpriseDatabaseState> expectedIds = new HashMap<>();
        try ( var tx = db.beginTx() )
        {
            makeDatabaseNode( tx, "A", true, expectedIds );
            makeDatabaseNode( tx, "B", false, expectedIds );
            makeDeletedDatabaseNode( tx, "C", expectedIds );
            tx.commit();
        }

        try ( var tx = db.beginTx() )
        {
            makeDatabaseNode( tx, "D", true, expectedIds );
            makeDeletedDatabaseNode( tx, "E", expectedIds );
            makeDeletedDatabaseNode( tx, "F", expectedIds );
            tx.commit();
        }

        var expectedNames = expectedIds.entrySet().stream().collect( Collectors.toMap( entry -> entry.getKey().name(), Map.Entry::getValue ) );

        // then
        assertEquals( expectedNames, dbmsModel.getDatabaseStates() );
    }

    private void makeDatabaseNode( Transaction tx, String databaseName, boolean online, HashMap<NamedDatabaseId,EnterpriseDatabaseState> expected )
    {
        UUID uuid = UUID.randomUUID();
        Node node = tx.createNode( DATABASE_LABEL );
        node.setProperty( DATABASE_NAME_PROPERTY, new NormalizedDatabaseName( databaseName ).name() );
        node.setProperty( DATABASE_STATUS_PROPERTY, online ? "online" : "offline" );
        node.setProperty( DATABASE_UUID_PROPERTY, uuid.toString() );
        var id = DatabaseIdFactory.from( databaseName, uuid );
        expected.put( id, online ? new EnterpriseDatabaseState( id, STARTED ) : new EnterpriseDatabaseState( id, STOPPED ) );
    }

    private void makeDeletedDatabaseNode( Transaction tx, String databaseName, HashMap<NamedDatabaseId,EnterpriseDatabaseState> expected )
    {
        UUID uuid = UUID.randomUUID();
        Node node = tx.createNode( DELETED_DATABASE_LABEL );
        node.setProperty( DATABASE_NAME_PROPERTY, new NormalizedDatabaseName( databaseName ).name() );
        node.setProperty( DATABASE_UUID_PROPERTY, uuid.toString() );
        var id = DatabaseIdFactory.from( databaseName, uuid );
        expected.put( id, new EnterpriseDatabaseState( id, DROPPED ) );
    }
}
