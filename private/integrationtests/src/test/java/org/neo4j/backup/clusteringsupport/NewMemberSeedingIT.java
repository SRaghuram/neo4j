/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.clusteringsupport;

import com.neo4j.causalclustering.common.DefaultCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Optional;

import org.neo4j.backup.clusteringsupport.backup_stores.BackupStore;
import org.neo4j.backup.clusteringsupport.backup_stores.BackupStoreWithSomeData;
import org.neo4j.backup.clusteringsupport.backup_stores.BackupStoreWithSomeDataButNoTransactionLogs;
import org.neo4j.backup.clusteringsupport.backup_stores.DefaultDatabasesBackup;
import org.neo4j.backup.clusteringsupport.backup_stores.EmptyBackupStore;
import org.neo4j.backup.clusteringsupport.cluster_load.ClusterLoad;
import org.neo4j.backup.clusteringsupport.cluster_load.NoLoad;
import org.neo4j.backup.clusteringsupport.cluster_load.SmallBurst;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.discovery.IpFamily;
import com.neo4j.causalclustering.discovery.SharedDiscoveryServiceFactory;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertFalse;
import static org.neo4j.backup.clusteringsupport.BackupUtil.restoreFromBackup;
import static com.neo4j.causalclustering.common.Cluster.dataMatchesEventually;

@RunWith( Parameterized.class )
public class NewMemberSeedingIT
{
    @Parameterized.Parameter()
    public BackupStore seedStore;

    @Parameterized.Parameter( 1 )
    public ClusterLoad intermediateLoad;

    private SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    private TestDirectory testDir = TestDirectory.testDirectory();
    private DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public RuleChain rules = RuleChain.outerRule( fileSystemRule ).around( testDir ).around( suppressOutput );

    private Cluster<?> cluster;
    private FileCopyDetector fileCopyDetector;
    private File baseBackupDir;

    @Parameterized.Parameters( name = "{0} with {1}" )
    public static Iterable<Object[]> data()
    {
        return combine( stores(), loads() );
    }

    private static Iterable<Object[]> combine( Iterable<BackupStore> stores, Iterable<ClusterLoad> loads )
    {
        ArrayList<Object[]> params = new ArrayList<>();
        for ( BackupStore store : stores )
        {
            for ( ClusterLoad load : loads )
            {
                params.add( new Object[]{store, load} );
            }
        }
        return params;
    }

    private static Iterable<ClusterLoad> loads()
    {
        return Arrays.asList( new NoLoad(), new SmallBurst() );
    }

    private static Iterable<BackupStore> stores()
    {
        return Arrays.asList( new EmptyBackupStore(), new BackupStoreWithSomeData(), new BackupStoreWithSomeDataButNoTransactionLogs() );
    }

    @Before
    public void setup()
    {
        this.fileCopyDetector = new FileCopyDetector();
        cluster = new DefaultCluster( testDir.directory( "cluster-b" ), 3, 0, new SharedDiscoveryServiceFactory(), emptyMap(), emptyMap(),
                emptyMap(), emptyMap(), Standard.LATEST_NAME, IpFamily.IPV4, false );
        baseBackupDir = testDir.directory( "backups" );
    }

    @After
    public void after()
    {
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void shouldSeedNewMemberToCluster() throws Exception
    {
        // given
        cluster.start();

        // when
        Optional<DefaultDatabasesBackup> backupsOpt = seedStore.generate( baseBackupDir, cluster );

        // then
        // possibly add load to cluster in between backup
        intermediateLoad.start( cluster );

        // when
        CoreClusterMember newCoreClusterMember = cluster.addCoreMemberWithId( 3 );
        if ( backupsOpt.isPresent() )
        {
            DefaultDatabasesBackup backups = backupsOpt.get();
            restoreFromBackup( backups.systemDb(), fileSystemRule.get(), newCoreClusterMember, GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
            restoreFromBackup( backups.defaultDb(), fileSystemRule.get(), newCoreClusterMember, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
        }

        // we want the new instance to seed from backup and not delete and re-download the store
        newCoreClusterMember.monitors().addMonitorListener( fileCopyDetector );
        newCoreClusterMember.start();

        // then
        intermediateLoad.stop();
        dataMatchesEventually( newCoreClusterMember, cluster.coreMembers() );
        assertFalse( fileCopyDetector.hasDetectedAnyFileCopied() );
    }
}
