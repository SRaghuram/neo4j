/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.clusteringsupport;

import com.neo4j.backup.stores.BackupStore;
import com.neo4j.backup.stores.BackupStoreWithSomeData;
import com.neo4j.backup.stores.BackupStoreWithSomeDataAndNoIdFiles;
import com.neo4j.backup.stores.EmptyBackupStore;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.load.ClusterLoad;
import com.neo4j.causalclustering.load.NoLoad;
import com.neo4j.causalclustering.load.SmallBurst;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.backup.BackupTestUtil.restoreFromBackup;
import static com.neo4j.causalclustering.common.DataMatching.dataMatchesEventually;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ClusterExtension
@TestInstance( PER_METHOD )
@ResourceLock( Resources.SYSTEM_OUT )
class NewMemberSeedingIT
{
    @Inject
    private TestDirectory testDir;
    @Inject
    private ClusterFactory clusterFactory;

    static List<Arguments> data()
    {
        var stores = List.of( new EmptyBackupStore(), new BackupStoreWithSomeData(), new BackupStoreWithSomeDataAndNoIdFiles() );
        var loads = List.of( new NoLoad(), new SmallBurst() );

        var result = new ArrayList<Arguments>();

        for ( var store : stores )
        {
            for ( var load : loads )
            {
                result.add( Arguments.of( store, load ) );
            }
        }

        return result;
    }

    @ParameterizedTest( name = "Store: {0}, Load: {1}" )
    @MethodSource( "data" )
    void shouldSeedNewMemberToCluster( BackupStore seedStore, ClusterLoad intermediateLoad ) throws Exception
    {
        // given
        var fileCopyDetector = new FileCopyDetector();
        var baseBackupDir = testDir.directoryPath( "backups" );
        var cluster = startCluster();

        // when
        var backupsOpt = seedStore.generate( baseBackupDir, cluster );

        // then
        // possibly add load to cluster in between backup
        intermediateLoad.start( cluster );

        // when
        var newCoreClusterMember = cluster.addCoreMemberWithIndex( 3 );
        if ( backupsOpt.isPresent() )
        {
            var backups = backupsOpt.get();
            restoreFromBackup( backups.systemDb(), testDir.getFileSystem(), newCoreClusterMember, SYSTEM_DATABASE_NAME );
            restoreFromBackup( backups.defaultDb(), testDir.getFileSystem(), newCoreClusterMember, DEFAULT_DATABASE_NAME );
        }

        // we want the new instance to seed from backup and not delete and re-download the store
        newCoreClusterMember.monitors().addMonitorListener( fileCopyDetector );
        newCoreClusterMember.start();

        // then
        intermediateLoad.stop();
        dataMatchesEventually( newCoreClusterMember, cluster.coreMembers() );
        assertFalse( fileCopyDetector.anyFileInDirectoryWithName( DEFAULT_DATABASE_NAME ) );
    }

    private Cluster startCluster() throws Exception
    {
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 );

        var cluster = clusterFactory.createCluster( clusterConfig );
        cluster.start();
        return cluster;
    }
}
