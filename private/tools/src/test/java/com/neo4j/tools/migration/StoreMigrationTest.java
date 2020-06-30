/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.tools.migration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.File;
import java.io.IOException;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.Unzip;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
class StoreMigrationTest
{
    @Inject
    private TestDirectory directory;
    private File migrationDir;

    @BeforeEach
    void setUp() throws IOException
    {
        migrationDir = directory.directory( "migration" );
        Unzip.unzip( getClass(), "3.4-store.zip", migrationDir );
    }

    @Test
    void storeMigrationToolShouldBeAbleToMigrateOldStore() throws Exception
    {
        StoreMigration.main( new String[]{migrationDir.getAbsolutePath()} );

        // after migration we can open store and do something
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( directory.directoryPath( "testdb" ) )
                .setConfig( GraphDatabaseSettings.logs_directory, directory.directory( "logs" ).toPath().toAbsolutePath() )
                .setConfig( GraphDatabaseSettings.transaction_logs_root_path, migrationDir.toPath().toAbsolutePath() )
                .build();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction transaction = database.beginTx() )
        {
            Node node = transaction.createNode();
            node.setProperty( "key", "value" );
            transaction.commit();
        }
        finally
        {
            managementService.shutdown();
        }
    }
}
