/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@Neo4jLayoutExtension
class BatchInserterEnterpriseConstraintIT
{
    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    void startBatchInserterOnTopOfEnterpriseDatabase() throws IOException
    {

        DatabaseManagementService
                managementService = new TestEnterpriseDatabaseManagementServiceBuilder( databaseLayout ).build();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.execute( "CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname, n.surname) IS NODE KEY" );
            transaction.commit();
        }
        managementService.shutdown();

        BatchInserter inserter = BatchInserters.inserter( databaseLayout );
        inserter.shutdown();
    }
}
