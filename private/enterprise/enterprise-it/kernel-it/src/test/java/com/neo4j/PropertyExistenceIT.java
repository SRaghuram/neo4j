/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
class PropertyExistenceIT
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void deletedNodesNotCheckedByExistenceConstraints()
    {
        DatabaseManagementService managementService =
                new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() ).build();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            try ( Transaction transaction = database.beginTx() )
            {
                transaction.execute( "CREATE CONSTRAINT ON (book:Book) ASSERT (book.isbn) IS NOT NULL" );
                transaction.commit();
            }

            try ( Transaction transaction = database.beginTx() )
            {
                transaction.execute( "CREATE (:label1 {name: \"Pelle\"})<-[:T1]-(:label2 {name: \"Elin\"})-[:T2]->(:label3)" );
                transaction.commit();
            }

            try ( Transaction transaction = database.beginTx() )
            {
                transaction.execute( "MATCH (n:label1 {name: \"Pelle\"})<-[r:T1]-(:label2 {name: \"Elin\"})-[:T2]->(:label3) DELETE r,n" );
                transaction.commit();
            }
        }
        finally
        {
            managementService.shutdown();
        }

    }
}
