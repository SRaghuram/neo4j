/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.enterprise;

import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;

import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( TestDirectoryExtension.class )
class BatchInserterEnterpriseConstraintIT
{
    @Inject
    private TestDirectory testDirectory;

    @Test
    void startBatchInserterOnTopOfEnterpriseDatabase() throws IOException
    {
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        DatabaseManagementService
                managementService = new TestCommercialDatabaseManagementServiceBuilder( databaseLayout.databaseDirectory() ).build();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            database.execute( "CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname, n.surname) IS NODE KEY" );
            transaction.success();
        }
        managementService.shutdown();

        BatchInserter inserter = BatchInserters.inserter( databaseLayout );
        inserter.shutdown();
    }
}
