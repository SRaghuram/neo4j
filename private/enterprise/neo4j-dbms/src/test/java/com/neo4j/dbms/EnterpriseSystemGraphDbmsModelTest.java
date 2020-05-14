/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import static org.neo4j.dbms.database.SystemGraphDbmsModel.UPDATED_AT_PROPERTY;

@ImpermanentDbmsExtension
class EnterpriseSystemGraphDbmsModelTest
{
    @Inject
    private DatabaseManagementService managementService;

    @Inject
    private GraphDatabaseService db;
    private final AtomicReference<DatabaseUpdates> updatedDatabases = new AtomicReference<>( DatabaseUpdates.EMPTY );
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
                updatedDatabases.getAndUpdate( previous -> mergeUpdates( previous, dbmsModel.updatedDatabases( txData ) ) );
            }
        } );
    }

    private DatabaseUpdates mergeUpdates( DatabaseUpdates left, DatabaseUpdates right )
    {
        var changed = Stream.concat( left.changed().stream(), right.changed().stream() ).collect( Collectors.toSet() );
        var touched = Stream.concat( left.touched().stream(), right.touched().stream() ).collect( Collectors.toSet() );
        var dropped = Stream.concat( left.dropped().stream(), right.dropped().stream() ).collect( Collectors.toSet() );
        return new DatabaseUpdates( changed, dropped, touched );
    }

    @Test
    void shouldDetectChangedDatabases()
    {
        // when
        HashMap<NamedDatabaseId,EnterpriseDatabaseState> expectedChanged = new HashMap<>();
        try ( var tx = db.beginTx() )
        {
            makeDatabaseNode( tx, "A", true, expectedChanged );
            makeDatabaseNode( tx, "B", true, expectedChanged );
            makeDatabaseNode( tx, "C", false, expectedChanged );
            tx.commit();
        }

        // then
        assertThat( updatedDatabases.get().changed() ).containsAll( expectedChanged.keySet() );

        // given
        updatedDatabases.set( DatabaseUpdates.EMPTY );

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
        assertThat( updatedDatabases.get().changed() ).containsAll( expectedDeleted.keySet() );
        assertThat( updatedDatabases.get().dropped() ).containsAll( expectedDeleted.keySet() );
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

    @Test
    void shouldDetectDatabasesOnlyTouchedInMultiUpdateTransactions()
    {
        // given
        var aState = new HashMap<NamedDatabaseId,EnterpriseDatabaseState>();
        try ( var tx = db.beginTx() )
        {
            makeDatabaseNode( tx, "A", true, aState );
            tx.commit();
        }

        var aName = new NormalizedDatabaseName( "A" ).name();
        updatedDatabases.set( DatabaseUpdates.EMPTY );

        // when
        var bState = new HashMap<NamedDatabaseId,EnterpriseDatabaseState>();
        try ( var tx = db.beginTx() )
        {
            makeDatabaseNode( tx, "B", true, bState );
            var aNode = tx.findNode( DATABASE_LABEL, DATABASE_NAME_PROPERTY, aName );
            if ( aNode != null )
            {
                aNode.setProperty( UPDATED_AT_PROPERTY, Instant.now().toString() );
            }
            tx.commit();
        }

        // then
        var touched = updatedDatabases.get().touched();
        var changed = updatedDatabases.get().changed();
        assertEquals( aState.keySet(), touched );
        assertThat( touched ).hasSize( 1 );
        assertEquals( bState.keySet(), changed );
        assertThat( changed ).hasSize( 1 );
    }

    @Test
    void shouldDetectDatabasesOnlyTouched()
    {
        // given
        var aState = new HashMap<NamedDatabaseId,EnterpriseDatabaseState>();
        try ( var tx = db.beginTx() )
        {
            makeDatabaseNode( tx, "A", true, aState );
            tx.commit();
        }

        var aName = new NormalizedDatabaseName( "A" ).name();
        updatedDatabases.set( DatabaseUpdates.EMPTY );

        // when
        var bState = new HashMap<NamedDatabaseId,EnterpriseDatabaseState>();
        try ( var tx = db.beginTx() )
        {
            makeDatabaseNode( tx, "B", true, bState );
            tx.commit();
        }

        // then
        var touched = updatedDatabases.get().touched();
        var changed = updatedDatabases.get().changed();
        assertThat( touched ).hasSize( 0 );
        assertEquals( bState.keySet(), changed );
        assertThat( changed ).hasSize( 1 );

        // when
        try ( var tx = db.beginTx() )
        {
            var aNode = tx.findNode( DATABASE_LABEL, DATABASE_NAME_PROPERTY, aName );
            if ( aNode != null )
            {
                aNode.setProperty( UPDATED_AT_PROPERTY, Instant.now().toString() );
            }
            tx.commit();
        }

        // then
        touched = updatedDatabases.get().touched();
        assertEquals( aState.keySet(), touched );
        assertThat( touched ).hasSize( 1 );
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
