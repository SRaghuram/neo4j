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
import com.neo4j.backup.stores.NoStore;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import org.neo4j.configuration.Config;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.backup.BackupTestUtil.restoreFromBackup;
import static com.neo4j.causalclustering.common.DataMatching.dataMatchesEventually;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;

@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ClusterExtension
@TestInstance( PER_METHOD )
class ClusterSeedingIT
{
    @Inject
    private TestDirectory testDir;
    @Inject
    private ClusterFactory clusterFactory;

    static Stream<Arguments> data()
    {
        return Stream.of(
                Arguments.of( new NoStore(), true ),
                Arguments.of( new EmptyBackupStore(), false ),
                Arguments.of( new BackupStoreWithSomeData(), false ),
                Arguments.of( new BackupStoreWithSomeDataAndNoIdFiles(), false ) );
    }

    @ParameterizedTest( name = "InitialStore: {0}, ShouldStoreCopy: {1}" )
    @MethodSource( "data" )
    void shouldSeedNewCluster( BackupStore initialStore, boolean shouldStoreCopy ) throws Exception
    {
        // given
        var baseBackupDir = testDir.directory( "backups" );
        var fileCopyDetector = new FileCopyDetector();

        var realCluster = createCluster();
        var backupCluster = createCluster();

        backupCluster.start();
        var backupsOpt = initialStore.generate( baseBackupDir, backupCluster );
        backupCluster.shutdown();

        if ( backupsOpt.isPresent() )
        {
            for ( var member : realCluster.coreMembers() )
            {
                var backups = backupsOpt.get();
                restoreFromBackup( backups.systemDb(), testDir.getFileSystem(), member, SYSTEM_DATABASE_NAME );
                restoreFromBackup( backups.defaultDb(), testDir.getFileSystem(), member, DEFAULT_DATABASE_NAME );
            }
        }

        // we want the cluster to seed from backup. No instance should delete and re-copy the store.
        realCluster.coreMembers().forEach( member -> member.monitors().addMonitorListener( fileCopyDetector ) );

        // when
        realCluster.start();

        // then
        if ( backupsOpt.isPresent() )
        {
            var backups = backupsOpt.get();
            var config = Config.defaults( default_database, backups.defaultDb().getFileName().toString() );
            var expectedDbRepresentation = DbRepresentation.of( DatabaseLayout.ofFlat( backups.defaultDb() ), config );
            dataMatchesEventually( expectedDbRepresentation, DEFAULT_DATABASE_NAME, realCluster.coreMembers() );
        }
        assertEquals( shouldStoreCopy, fileCopyDetector.anyFileInDirectoryWithName( DEFAULT_DATABASE_NAME ) );
    }

    private Cluster createCluster()
    {
        var clusterConfig = clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 0 );

        return clusterFactory.createCluster( clusterConfig );
    }
}
