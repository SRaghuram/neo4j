/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.causalclustering.core.state.CoreStateStorageFactory;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;

import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ClusterExtension
@ExtendWith( SuppressOutputExtension.class )
class ClusterStateMigrationIT
{
    @Inject
    private ClusterFactory clusterFactory;

    private Cluster cluster;

    @BeforeAll
    void setup() throws Exception
    {
        var clusterConfig = clusterConfig().withNumberOfCoreMembers( 2 ).withNumberOfReadReplicas( 0 );
        cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
    }

    @Test
    void shouldCreateClusterStateVersion() throws Exception
    {
        for ( var member : cluster.coreMembers() )
        {
            var versionStorage = clusterStateVersionStorage( member );
            assertTrue( versionStorage.exists() );
            assertEquals( 1, versionStorage.readState() );
        }
    }

    @Test
    void shouldRecreateClusterStateWhenVersionIsAbsent() throws Exception
    {
        // memorize current member IDs
        var serverIdToMemberIdMap = cluster.coreMembers()
                .stream()
                .collect( toMap( CoreClusterMember::serverId, CoreClusterMember::id ) );

        // collect all version files
        var clusterStateVersionFiles = cluster.coreMembers()
                .stream()
                .map( ClusterStateMigrationIT::clusterStateLayout )
                .map( ClusterStateLayout::clusterStateVersionFile )
                .collect( toList() );

        cluster.shutdown();

        // remove all version files to force migrator to recreate cluster-state directories
        // this is expected because cluster-state directory without a version file is considered to be from an old neo4j version
        clusterStateVersionFiles.forEach( FileUtils::deleteFile );

        cluster.start();

        for ( var member : cluster.coreMembers() )
        {
            // all members should new member IDs as result of cluster-state recreation
            assertTrue( serverIdToMemberIdMap.containsKey( member.serverId() ) );
            assertNotEquals( serverIdToMemberIdMap.get( member.serverId() ), member.id() );

            // version files should exist
            var versionStorage = clusterStateVersionStorage( member );
            assertTrue( versionStorage.exists() );
            assertEquals( 1, versionStorage.readState() );
        }
    }

    @Test
    void shouldFailWhenClusterStateVersionIsWrong() throws Exception
    {
        // collect all version storages
        var clusterStateVersionStorages = cluster.coreMembers()
                .stream()
                .map( ClusterStateMigrationIT::clusterStateVersionStorage )
                .collect( toList() );

        cluster.shutdown();

        // write illegal version in all storages
        for ( var storage : clusterStateVersionStorages )
        {
            storage.writeState( 42L );
        }

        // cluster should not be able to start
        assertThrows( Exception.class, cluster::start );

        // write the correct version and restart the cluster
        for ( var storage : clusterStateVersionStorages )
        {
            storage.writeState( 1L );
        }
        cluster.start();
    }

    private static SimpleStorage<Long> clusterStateVersionStorage( ClusterMember member )
    {
        var storageFactory = storageFactory( member );
        return storageFactory.createClusterStateVersionStorage();
    }

    private static CoreStateStorageFactory storageFactory( ClusterMember member )
    {
        var clusterStateLayout = clusterStateLayout( member );
        var fs = member.defaultDatabase().getDependencyResolver().resolveDependency( FileSystemAbstraction.class );
        var logProvider = FormattedLogProvider.toOutputStream( System.out );
        return new CoreStateStorageFactory( fs, clusterStateLayout, logProvider, Config.defaults() );
    }

    private static ClusterStateLayout clusterStateLayout( ClusterMember member )
    {
        var dataDir = new File( member.homeDir(), "data" );
        return ClusterStateLayout.of( dataDir );
    }
}
