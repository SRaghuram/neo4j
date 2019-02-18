/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CoreStateStorageServiceTest
{
    @Rule
    public final TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public final FileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Test
    public void shouldMigrateDatabaseSpecificState() throws IOException
    {
        // given
        File clusterStateDir = makeDir( testDirectory.directory(), "cluster-state" );

        ClusterStateDirectory clusterState = new ClusterStateDirectory( fs, testDirectory.directory(), false );
        clusterState.initialize();

        CoreStateStorageService coreStateStorage =
                new CoreStateStorageService( fs, clusterState, new LifeSupport(), NullLogProvider.getInstance(), Config.defaults() );

        String databaseName = "graph.db";

        File oldIdAllocDir = makeDir( clusterStateDir, CoreStateFiles.ID_ALLOCATION.directoryName() );
        File oldLockTokenDir = makeDir( clusterStateDir, CoreStateFiles.LOCK_TOKEN.directoryName() );
        File oldIdFile = makeFile( oldIdAllocDir, "id-file" );
        File oldLockFile = makeFile( oldLockTokenDir, "lock-file" );

        // when
        coreStateStorage.migrateIfNecessary( databaseName );

        File dbStateDir = new File( new File( clusterStateDir, "db" ), databaseName );
        File newIdFile = new File( new File( dbStateDir, CoreStateFiles.ID_ALLOCATION.directoryName() ), "id-file" );
        File newLockFile = new File( new File( dbStateDir, CoreStateFiles.LOCK_TOKEN.directoryName() ), "lock-file" );

        // then
        assertFalse( fs.fileExists( oldIdAllocDir ) );
        assertFalse( fs.fileExists( oldLockTokenDir ) );
        assertFalse( fs.fileExists( oldIdFile ) );
        assertFalse( fs.fileExists( oldLockFile ) );

        assertTrue( fs.fileExists( newIdFile ) );
        assertTrue( fs.fileExists( newLockFile ) );
    }

    private final FileSystemAbstraction fs = fileSystemRule.get();

    private File makeFile( File baseDirectory, String fileName ) throws IOException
    {
        File file = new File( baseDirectory, fileName );
        fs.create( file ).close();
        return file;
    }

    private File makeDir( File baseDirectory, String directoryName )
    {
        File directory = new File( baseDirectory, directoryName );
        fs.mkdir( directory );
        return directory;
    }
}
