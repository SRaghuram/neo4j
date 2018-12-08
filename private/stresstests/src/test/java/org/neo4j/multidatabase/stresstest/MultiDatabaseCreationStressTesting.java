/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.multidatabase.stresstest;

import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;
import com.neo4j.dbms.database.MultiDatabaseManager;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import static java.lang.System.getProperty;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.neo4j.helper.StressTestingHelper.ensureExistsAndEmpty;
import static org.neo4j.helper.StressTestingHelper.fromEnv;
import static org.neo4j.io.fs.FileUtils.deleteRecursively;

class MultiDatabaseCreationStressTesting
{
    private static final String DEFAULT_WORKING_DIR = new File( getProperty( "java.io.tmpdir" ) ).getPath();

    @Test
    void multiDatabaseLifecycleStressTest() throws IOException
    {
        String workingDirectory = fromEnv( "MULTIDATABASE_STRESS_WORKING_DIRECTORY", DEFAULT_WORKING_DIR );
        File storeDirectory = new File( workingDirectory, "databases" );
        deleteRecursively( storeDirectory );
        ensureExistsAndEmpty( storeDirectory );

        GraphDatabaseService databaseService = new CommercialGraphDatabaseFactory().newEmbeddedDatabase( storeDirectory );

        try
        {
            DatabaseManager databaseManager = getDatabaseManager( (GraphDatabaseAPI) databaseService );
            assertThat( databaseManager, instanceOf( MultiDatabaseManager.class ) );

            String testDatabase = "testDatabase";
            databaseManager.createDatabase( testDatabase );
            int counter = 0;
            while ( true )
            {
                databaseManager.stopDatabase( testDatabase );
                databaseManager.startDatabase( testDatabase );
                System.out.println( "cycle: " + counter++ );
            }

        }
        finally
        {
            databaseService.shutdown();
            deleteRecursively( storeDirectory );
        }
    }

    private DatabaseManager getDatabaseManager( GraphDatabaseAPI databaseService )
    {
        return databaseService.getDependencyResolver().resolveDependency( DatabaseManager.class );
    }
}
