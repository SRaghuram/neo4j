/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;

public class PropertyExistenceIT
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();

    @Test
    public void deletedNodesNotCheckedByExistenceConstraints()
    {
        GraphDatabaseService database = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabase( testDirectory.directory() );
        try
        {
            try ( Transaction transaction = database.beginTx() )
            {
                database.execute( "CREATE CONSTRAINT ON (book:Book) ASSERT exists(book.isbn)" );
                transaction.success();
            }

            try ( Transaction transaction = database.beginTx() )
            {
                database.execute( "CREATE (:label1 {name: \"Pelle\"})<-[:T1]-(:label2 {name: \"Elin\"})-[:T2]->(:label3)" );
                transaction.success();
            }

            try ( Transaction transaction = database.beginTx() )
            {
                database.execute( "MATCH (n:label1 {name: \"Pelle\"})<-[r:T1]-(:label2 {name: \"Elin\"})-[:T2]->(:label3) DELETE r,n" );
                transaction.success();
            }
        }
        finally
        {
            database.shutdown();
        }

    }
}
