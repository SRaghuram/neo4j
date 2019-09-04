/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import org.junit.Test;

import java.io.File;

import org.neo4j.causalclustering.common.StubLocalDatabase;
import org.neo4j.causalclustering.common.StubLocalDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.NullLogProvider;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusterStateCleanerTest
{

    @Test
    public void shouldDeleteClusterStateInTheEventOfAnyEmptyDatabase() throws Throwable
    {
        // given
        StubLocalDatabaseService dbService = new StubLocalDatabaseService();

        dbService.givenDatabaseWithConfig()
                .withDatabaseName( GraphDatabaseSettings.SYSTEM_DATABASE_NAME )
                .withEmptyStore( false )
                .register();

        dbService.givenDatabaseWithConfig()
                .withDatabaseName( GraphDatabaseSettings.DEFAULT_DATABASE_NAME )
                .withEmptyStore( true )
                .register();

        CoreStateStorageService storageService = mock( CoreStateStorageService.class );
        ClusterStateDirectory clusterStateDirectory = mock( ClusterStateDirectory.class );
        when( storageService.clusterStateDirectory() ).thenReturn( clusterStateDirectory );
        when( clusterStateDirectory.isEmpty() ).thenReturn( false );
        File stateDir = mock( File.class );
        when( clusterStateDirectory.get() ).thenReturn( stateDir );
        FileSystemAbstraction fs = mock( FileSystemAbstraction.class );

        ClusterStateCleaner clusterStateCleaner = new ClusterStateCleaner( dbService, storageService, fs, NullLogProvider.getInstance() );

        // when
        clusterStateCleaner.init();
        // Init again and make sure that when a database is *not* empty that the cluster state does *not* get cleared.
        StubLocalDatabase db = (StubLocalDatabase) dbService.get( GraphDatabaseSettings.DEFAULT_DATABASE_NAME ).orElseThrow( IllegalStateException::new );
        db.setEmpty( false );
        clusterStateCleaner.init();

        // then
        verify( clusterStateDirectory, times( 1 ) ).clear( fs );
    }
}
