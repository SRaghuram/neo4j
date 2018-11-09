/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ClusterStateDirectoryTest
{
    @Rule
    public DefaultFileSystemRule fsRule = new DefaultFileSystemRule();
    private FileSystemAbstraction fs = fsRule.get();

    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( fs );

    private File dataDir;
    private File stateDir;

    @Before
    public void setup()
    {
        dataDir = testDirectory.directory( "data" );
        stateDir = new File( dataDir, ClusterStateDirectory.CLUSTER_STATE_DIRECTORY_NAME );
    }

    @Test
    public void shouldMigrateClusterStateFromStoreDir() throws Exception
    {
        // given
        File storeDir = new File( new File( dataDir, "databases" ), GraphDatabaseSettings.DEFAULT_DATABASE_NAME );

        String fileName = "file";

        File oldStateDir = new File( storeDir, ClusterStateDirectory.CLUSTER_STATE_DIRECTORY_NAME );
        File oldClusterStateFile = new File( oldStateDir, fileName );

        fs.mkdirs( oldStateDir );
        fs.create( oldClusterStateFile ).close();

        // when
        ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( dataDir, storeDir, false );
        clusterStateDirectory.initialize( fs, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );

        // then
        assertEquals( clusterStateDirectory.get(), stateDir );
        assertTrue( fs.fileExists( new File( clusterStateDirectory.get(), fileName ) ) );
    }

    @Test
    public void shouldHandleCaseOfStoreDirBeingDataDir() throws Exception
    {
        // given
        File storeDir = dataDir;

        String fileName = "file";

        File oldStateDir = new File( storeDir, ClusterStateDirectory.CLUSTER_STATE_DIRECTORY_NAME );
        File oldClusterStateFile = new File( oldStateDir, fileName );

        fs.mkdirs( oldStateDir );
        fs.create( oldClusterStateFile ).close();

        // when
        ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( dataDir, storeDir, false );
        clusterStateDirectory.initialize( fs, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );

        // then
        assertEquals( clusterStateDirectory.get(), stateDir );
        assertTrue( fs.fileExists( new File( clusterStateDirectory.get(), fileName ) ) );
    }

    @Test
    public void shouldMigrateDatabaseStateToSubDirectory() throws Exception
    {
        //given
        fs.mkdirs( stateDir );
        File rootIdAlloc = CoreStateFiles.ID_ALLOCATION.at( stateDir );
        File rootLockToken = CoreStateFiles.LOCK_TOKEN.at( stateDir );
        fs.create( rootIdAlloc );
        fs.create( rootLockToken );
        String databaseName = "arbitrary.db";
        File expectedDatabaseStateDirectory = new File( new File( stateDir, "db" ), databaseName );

        //when
        ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( dataDir, false );
        clusterStateDirectory.initialize( fs, databaseName );

        //then
        assertTrue( fs.fileExists( expectedDatabaseStateDirectory ) );
        assertTrue( fs.fileExists( CoreStateFiles.ID_ALLOCATION.at( expectedDatabaseStateDirectory ) ) );
        assertTrue( fs.fileExists( CoreStateFiles.LOCK_TOKEN.at( expectedDatabaseStateDirectory ) ) );
        assertFalse( fs.fileExists( rootIdAlloc ) );
        assertFalse( fs.fileExists( rootLockToken ) );
    }

    @Test
    public void shouldThrowOnMissingDatabaseFilesDuringMigration() throws Exception
    {
        //given
        fs.mkdirs( stateDir );
        File rootIdAlloc = CoreStateFiles.ID_ALLOCATION.at( stateDir );
        // missing: CoreStateFiles.LOCK_TOKEN.at( stateDir );

        fs.create( rootIdAlloc );
        String databaseName = "graph.db";

        ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( dataDir, false );

        try
        {
            //when
            clusterStateDirectory.initialize( fs, databaseName );
            fail();
        }
        catch ( ClusterStateException e )
        {
            assertThat( e.getMessage(), equalTo( ClusterStateDirectory.MISSING_FILES_MESSAGE ) );
        }
    }

    @Test
    public void shouldThrowOnPreExistingDatabaseStateDuringMigration() throws Exception
    {
        //given
        fs.mkdirs( stateDir );
        File rootIdAlloc = CoreStateFiles.ID_ALLOCATION.at( stateDir );
        File rootLockToken = CoreStateFiles.LOCK_TOKEN.at( stateDir );
        fs.create( rootIdAlloc );
        fs.create( rootLockToken );
        String databaseName = "graph.db";

        File existingDatabaseStateDirectory = new File( new File( stateDir, "db" ), databaseName );
        fs.mkdirs( existingDatabaseStateDirectory );

        ClusterStateDirectory clusterStateDirectory = new ClusterStateDirectory( dataDir, false );

        try
        {
            //when
            clusterStateDirectory.initialize( fs, databaseName );
            fail();
        }
        catch ( ClusterStateException e )
        {
            assertThat( e.getMessage(), equalTo( ClusterStateDirectory.OVERLAPPING_DATABASE_STATE_MESSAGE ) );
        }
    }
}
