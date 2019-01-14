/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.impl.enterprise;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.EnterpriseGraphDatabaseFactory;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

@ExtendWith( TestDirectoryExtension.class )
class BatchInserterEnterpriseConstraintIT
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void startBatchInserterOnTopOfEnterpriseDatabase() throws IOException
    {
        File databaseDir = testDirectory.databaseDir();
        GraphDatabaseService database = new EnterpriseGraphDatabaseFactory().newEmbeddedDatabase( databaseDir );
        try ( Transaction transaction = database.beginTx() )
        {
            database.execute( "CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname, n.surname) IS NODE KEY" );
            transaction.success();
        }
        database.shutdown();

        BatchInserter inserter = BatchInserters.inserter( databaseDir );
        inserter.shutdown();
    }
}
