/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterators.count;

@EnterpriseDbmsExtension
class MultiDatabaseTransactionBridgeIT
{
    @Inject
    private GraphDatabaseAPI db;
    @Inject
    private DatabaseManagementService managementService;

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
            NotInTransactionException exception =
                    assertThrows( NotInTransactionException.class,
                            () -> systemNode.createRelationshipTo( systemNode, RelationshipType.withName( "any" ) ) );
            assertThat( exception.getMessage() ).contains( "The transaction has been closed." );
        }
    }

    @Test
    void queryOnSystemDbInsideSystemDbTransaction()
    {
        GraphDatabaseService systemDb = managementService.database( SYSTEM_DATABASE_NAME );
        try ( Transaction transaction = systemDb.beginTx() )
        {
            assertThat( count( transaction.execute( "SHOW DATABASES" ) ) ).isGreaterThanOrEqualTo( 2L );
        }
    }
}
