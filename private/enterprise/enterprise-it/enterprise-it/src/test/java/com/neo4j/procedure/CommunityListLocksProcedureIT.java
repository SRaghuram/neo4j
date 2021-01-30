/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.coreapi.InternalTransaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;

@EnterpriseDbmsExtension( configurationCallback = "configure" )
public class CommunityListLocksProcedureIT
{
    @Inject
    private GraphDatabaseService database;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( GraphDatabaseInternalSettings.lock_manager, "community" );
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void listSharedLabelLock( String runtime )
    {
        var markerLabel = Label.label( "Marker" );
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.createNode( markerLabel );
            transaction.commit();
        }
        long labelId = getLabelId( markerLabel );

        try ( Transaction transaction = database.beginTx() )
        {
            var result = transaction.execute( "MATCH (n:Marker) RETURN n" );
            var procedureResult = transaction.execute( format( "CYPHER runtime=%s CALL db.listLocks()", runtime ) );
            var labelLockMap = procedureResult.next();
            assertThat( labelLockMap ).containsEntry( "mode", "SHARED" )
                                      .containsEntry( "resourceType", "LABEL" )
                                      .containsEntry( "resourceId", labelId )
                                      .containsEntry( "transactionId", database.databaseName() + "-transaction-4" );
            assertFalse( procedureResult.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void listExclusiveNodeLock( String runtime )
    {
        long nodeId;
        try ( Transaction transaction = database.beginTx() )
        {
            nodeId = transaction.createNode().getId();
            transaction.commit();
        }
        try ( Transaction transaction = database.beginTx() )
        {
            var result = transaction.execute( "MATCH (n) SET n.property=4 RETURN n" );
            var procedureResult = transaction.execute( format( "CYPHER runtime=%s CALL db.listLocks()", runtime ) );
            var labelLockMap = procedureResult.next();
            assertThat( labelLockMap ).containsEntry( "mode", "EXCLUSIVE" )
                                      .containsEntry( "resourceType", "NODE" )
                                      .containsEntry( "resourceId", nodeId )
                                      .containsEntry( "transactionId", database.databaseName() + "-transaction-2" );
            assertFalse( procedureResult.hasNext() );
        }
    }

    @ParameterizedTest
    @ValueSource( strings = {"INTERPRETED", "SLOTTED", "PIPELINED"} )
    void listSharedAndExclusiveLockOnSameNode( String runtime )
    {
        long nodeId;
        try ( Transaction transaction = database.beginTx() )
        {
            nodeId = transaction.createNode().getId();
            transaction.commit();
        }
        try ( Transaction transaction = database.beginTx() )
        {
            var node = transaction.getNodeById( nodeId );
            transaction.acquireReadLock( node );
            transaction.acquireReadLock( node );
            transaction.acquireReadLock( node );
            var result = transaction.execute( "MATCH (n) SET n.property=4 RETURN n" );
            var procedureResult = transaction.execute( format( "CYPHER runtime=%s CALL db.listLocks()", runtime ) );
            var labelLockMap = procedureResult.next();
            assertThat( labelLockMap ).containsEntry( "mode", "EXCLUSIVE" )
                                      .containsEntry( "resourceType", "NODE" )
                                      .containsEntry( "resourceId", nodeId )
                                      .containsEntry( "transactionId", database.databaseName() + "-transaction-2" );
            assertFalse( procedureResult.hasNext() );
        }
    }

    private long getLabelId( Label markerLabel )
    {
        try ( InternalTransaction tx = (InternalTransaction) database.beginTx() )
        {
            return tx.kernelTransaction().tokenRead().nodeLabel( markerLabel.name() );
        }
    }
}

