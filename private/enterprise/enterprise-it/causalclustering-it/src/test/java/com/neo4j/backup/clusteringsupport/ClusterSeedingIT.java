/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.clusteringsupport;

import com.neo4j.backup.stores.BackupStore;
import com.neo4j.backup.stores.BackupStoreWithSomeData;
import com.neo4j.backup.stores.BackupStoreWithSomeDataAndNoIdFiles;
import com.neo4j.backup.stores.DefaultDatabasesBackup;
import com.neo4j.backup.stores.EmptyBackupStore;
import com.neo4j.backup.stores.NoStore;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.discovery.IpFamily;
import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.rule.SuppressOutput;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static com.neo4j.backup.BackupTestUtil.restoreFromBackup;
import static com.neo4j.causalclustering.common.Cluster.dataMatchesEventually;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;

@RunWith( Parameterized.class )
public class ClusterSeedingIT
{
    @Parameterized.Parameter()
    public BackupStore initialStore;

    @Parameterized.Parameter( 1 )
    public boolean shouldStoreCopy;

    private SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    private TestDirectory testDir = TestDirectory.testDirectory();
    private DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Rule
    public RuleChain rules = RuleChain.outerRule( fileSystemRule ).around( testDir ).around( suppressOutput );

    private Cluster backupCluster;
    private Cluster cluster;
    private FileCopyDetector fileCopyDetector;
    private File baseBackupDir;

    @Parameterized.Parameters( name = "{0}" )
    public static Object[][] data()
    {
        return new Object[][]{
                {new NoStore(), true},
                {new EmptyBackupStore(), false},
                {new BackupStoreWithSomeData(), false},
                {new BackupStoreWithSomeDataAndNoIdFiles(), false},
        };
    }

    @Before
    public void setup()
    {
        this.fileCopyDetector = new FileCopyDetector();
        backupCluster = new Cluster( testDir.directory( "cluster-for-backup" ), 3, 0,
                new AkkaDiscoveryServiceFactory(), emptyMap(), emptyMap(), emptyMap(), emptyMap(), Standard
                .LATEST_NAME, IpFamily.IPV4, false );

        cluster = new Cluster( testDir.directory( "cluster-b" ), 3, 0,
                new AkkaDiscoveryServiceFactory(), emptyMap(), emptyMap(), emptyMap(), emptyMap(), Standard.LATEST_NAME,
                IpFamily.IPV4, false );

        baseBackupDir = testDir.directory( "backups" );
    }

    @After
    public void after()
    {
        if ( backupCluster != null )
        {
            backupCluster.shutdown();
        }
        if ( cluster != null )
        {
            cluster.shutdown();
        }
    }

    @Test
    public void shouldSeedNewCluster() throws Exception
    {
        // given
        backupCluster.start();
        Optional<DefaultDatabasesBackup> backupsOpt = initialStore.generate( baseBackupDir, backupCluster );
        backupCluster.shutdown();

        if ( backupsOpt.isPresent() )
        {
            for ( CoreClusterMember member : cluster.coreMembers() )
            {
                DefaultDatabasesBackup backups = backupsOpt.get();
                restoreFromBackup( backups.systemDb(), fileSystemRule.get(), member, GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
                restoreFromBackup( backups.defaultDb(), fileSystemRule.get(), member, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
            }
        }

        // we want the cluster to seed from backup. No instance should delete and re-copy the store.
        cluster.coreMembers().forEach( ccm -> ccm.monitors().addMonitorListener( fileCopyDetector ) );

        // when
        cluster.start();

        // then
        if ( backupsOpt.isPresent() )
        {
            DefaultDatabasesBackup backups = backupsOpt.get();
            Config config = Config.defaults( GraphDatabaseSettings.default_database, backups.defaultDb().getName() );
            dataMatchesEventually( DbRepresentation.of( DatabaseLayout.of( backups.defaultDb() ), config ), cluster.coreMembers() );
        }
        assertEquals( shouldStoreCopy, fileCopyDetector.hasDetectedAnyFileCopied() );
    }
}
