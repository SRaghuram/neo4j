/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dmbs.database;

import com.neo4j.causalclustering.discovery.CommercialCluster;
import com.neo4j.causalclustering.discovery.SslHazelcastDiscoveryServiceFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( TestDirectoryExtension.class )
class MultiDatabaseManagerIT
{
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private CommercialCluster cluster;

    @BeforeEach
    void setUp() throws TimeoutException, ExecutionException, InterruptedException
    {
        //TODO:replace with commercial enterprise db instance as soon as it will be available
        cluster = new CommercialCluster( testDirectory.absolutePath(), 3, 0, new SslHazelcastDiscoveryServiceFactory(), emptyMap(), emptyMap(), emptyMap(),
                emptyMap(), Standard.LATEST_NAME, IpFamily.IPV4, false );
        cluster.start();
        database = cluster.awaitLeader().database();
    }

    @AfterEach
    void tearDown()
    {
        cluster.shutdown();
    }

    @Test
    void createDatabase()
    {
        DatabaseManager databaseManager = getDatabaseManager();
        String databaseName = "testDatabase";
        GraphDatabaseFacade database1 = databaseManager.createDatabase( databaseName );

        assertNotNull( database1 );
        assertEquals( databaseName, database1.databaseDirectory().getName() );
    }

    @Test
    void lookupNotExistingDatabase()
    {
        DatabaseManager databaseManager = getDatabaseManager();
        Optional<GraphDatabaseFacade> database = databaseManager.getDatabaseFacade( "testDatabase" );
        assertFalse( database.isPresent() );
    }

    @Test
    void lookupExistingDatabase()
    {
        DatabaseManager databaseManager = getDatabaseManager();
        Optional<GraphDatabaseFacade> database = databaseManager.getDatabaseFacade( DatabaseManager.DEFAULT_DATABASE_NAME );
        assertTrue( database.isPresent() );
    }

    @Test
    void createAndShutdownDatabase()
    {
        DatabaseManager databaseManager = getDatabaseManager();
        String databaseName = "databaseToShutdown";
        GraphDatabaseFacade database = databaseManager.createDatabase( databaseName );

        Optional<GraphDatabaseFacade> databaseLookup = databaseManager.getDatabaseFacade( databaseName );
        assertTrue( databaseLookup.isPresent() );
        assertEquals( database, databaseLookup.get() );

        databaseManager.shutdownDatabase( databaseName );
        assertFalse( databaseManager.getDatabaseFacade( databaseName ).isPresent() );
    }

    private DatabaseManager getDatabaseManager()
    {
        return ((GraphDatabaseAPI)database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }
}
