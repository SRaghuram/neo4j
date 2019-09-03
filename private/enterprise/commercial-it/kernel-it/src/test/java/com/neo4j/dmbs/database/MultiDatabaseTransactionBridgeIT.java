/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dmbs.database;

import com.neo4j.test.extension.CommercialDbmsExtension;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.count;

@CommercialDbmsExtension
class MultiDatabaseTransactionBridgeIT
{
    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private DatabaseManagementService managementService;

    @Test
    void queryOnSystemDbInsideOtherDbTransaction()
    {
        GraphDatabaseService systemDb = managementService.database( SYSTEM_DATABASE_NAME );
        try ( Transaction transaction = db.beginTx() )
        {
            TransactionFailureException exception = assertThrows( TransactionFailureException.class, () -> systemDb.execute( "SHOW DATABASES" ) );
            assertThat( exception.getMessage(), containsString( "transaction already bound to this thread" ) );
        }
    }

    @Test
    void lookupNodeOnSystemDbInsideOtherDbTransaction()
    {
        GraphDatabaseService systemDb = managementService.database( SYSTEM_DATABASE_NAME );
        try ( Transaction transaction = db.beginTx() )
        {
            TransactionFailureException exception = assertThrows( TransactionFailureException.class, () -> systemDb.getNodeById(1) );
            assertThat( exception.getMessage(), containsString( "transaction already bound to this thread" ) );
        }
    }

    @Test
    void lookupNodesByLabelOnSystemDbInsideOtherDbTransaction()
    {
        GraphDatabaseService systemDb = managementService.database( SYSTEM_DATABASE_NAME );
        try ( Transaction transaction = db.beginTx() )
        {
            TransactionFailureException exception = assertThrows( TransactionFailureException.class, () -> systemDb.findNodes( Label.label( "any" ) ) );
            assertThat( exception.getMessage(), containsString( "transaction already bound to this thread" ) );
        }
    }

    @Test
    void lookupTokensOnSystemDbInsideOtherDbTransaction()
    {
        GraphDatabaseService systemDb = managementService.database( SYSTEM_DATABASE_NAME );
        try ( Transaction transaction = db.beginTx() )
        {
            TransactionFailureException exception = assertThrows( TransactionFailureException.class, transaction::getAllLabels );
            assertThat( exception.getMessage(), containsString( "transaction already bound to this thread" ) );
        }
    }

    @Test
    void beginTransactionOnSystemDbInsideOtherDbTransaction()
    {
        GraphDatabaseService systemDb = managementService.database( SYSTEM_DATABASE_NAME );
        try ( Transaction transaction = db.beginTx() )
        {
            TransactionFailureException exception = assertThrows( TransactionFailureException.class, systemDb::beginTx );
            assertThat( exception.getMessage(), containsString( "Fail to start new transaction. Already have transaction in the context." ) );
        }
    }

    @Test
    void createRelationshipOnSystemDbNodeInsideOtherDbTransaction()
    {
        GraphDatabaseService systemDb = managementService.database( SYSTEM_DATABASE_NAME );
        Node systemNode;
        try ( Transaction transaction = systemDb.beginTx() )
        {
            systemNode = transaction.createNode();
            transaction.commit();
        }
        try ( Transaction transaction = db.beginTx() )
        {
            TransactionFailureException exception =
                    assertThrows( TransactionFailureException.class, () -> systemNode.createRelationshipTo( systemNode, RelationshipType.withName( "any" ) ) );
            assertThat( exception.getMessage(), containsString( "transaction already bound to this thread" ) );
        }
    }

    @Test
    void queryOnSystemDbInsideSystemDbTransaction()
    {
        GraphDatabaseService systemDb = managementService.database( SYSTEM_DATABASE_NAME );
        try ( Transaction transaction = systemDb.beginTx() )
        {
            assertThat( count( systemDb.execute( "SHOW DATABASES" ) ), greaterThanOrEqualTo( 2L ) );
        }
    }
}
