/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.io.File;

import org.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.StoreLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusterStateCleanerTest
{
    private static final FileSystemRule fileSystemRule = new EphemeralFileSystemRule();
    private static final TestDirectory testDirectory = TestDirectory.testDirectory( fileSystemRule );

    @Rule
    public final RuleChain ruleChain = RuleChain.outerRule( fileSystemRule ).around( testDirectory );

    @Test
    public void shouldDeleteClusterStateInTheEventOfAnyEmptyDatabase() throws Throwable
    {
        // given

        // a mock core state storage service which returns a non-empty cluster state directory
        CoreStateStorageService storageService = mock( CoreStateStorageService.class );
        ClusterStateDirectory clusterStateDirectory = mock( ClusterStateDirectory.class );
        when( storageService.clusterStateDirectory() ).thenReturn( clusterStateDirectory );
        when( clusterStateDirectory.isEmpty() ).thenReturn( false );
        File stateDir = mock( File.class );
        when( clusterStateDirectory.get() ).thenReturn( stateDir );

        // and a store layout / store files which report empty databases
        StoreLayout storeLayout = testDirectory.storeLayout();
        FileSystemAbstraction fs = fileSystemRule.get();
        StoreFiles storeFiles = mock( StoreFiles.class );
        when( storeFiles.isEmpty( any( File.class ), anyCollection() ) ).thenReturn( true ).thenReturn( false );

        ClusterStateCleaner clusterStateCleaner = new ClusterStateCleaner( storeFiles, storeLayout, storageService, fs,
                NullLogProvider.getInstance(), Config.defaults() );

        // when
        clusterStateCleaner.init();
        // Init again and make sure that when a database is *not* empty that the cluster state does *not* get cleared.
        clusterStateCleaner.init();

        // then
        verify( clusterStateDirectory, times( 1 ) ).clear( fs );
    }
}
