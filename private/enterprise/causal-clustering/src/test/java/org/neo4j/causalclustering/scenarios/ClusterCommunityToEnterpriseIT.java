/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.scenarios;

import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.common.EnterpriseCluster;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.discovery.SharedDiscoveryServiceFactory;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.Collections.emptyMap;
import static org.neo4j.causalclustering.common.Cluster.dataMatchesEventually;

public class ClusterCommunityToEnterpriseIT
{
    private Cluster<?> cluster;
    private FileSystemAbstraction fsa;

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();
    @Rule
    public DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Before
    public void setup()
    {
        fsa = fileSystemRule.get();

        cluster = new EnterpriseCluster( testDir.directory( "cluster" ), 3, 0,
                new SharedDiscoveryServiceFactory(), emptyMap(), emptyMap(), emptyMap(), emptyMap(), HighLimit.NAME,
                IpFamily.IPV4, false );
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
    public void shouldRestoreBySeedingAllMembers() throws Throwable
    {
        // given
        GraphDatabaseService database = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder( testDir.storeDir() )
                .setConfig( GraphDatabaseSettings.allow_upgrade, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.record_format, HighLimit.NAME )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Boolean.FALSE.toString() )
                .newGraphDatabase();
        database.shutdown();
        Config config = Config.defaults( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
        DbRepresentation before = DbRepresentation.of( testDir.storeDir(), config );

        // when
        fsa.copyRecursively( testDir.databaseDir(), cluster.getCoreMemberById( 0 ).databaseDirectory() );
        fsa.copyRecursively( testDir.databaseDir(), cluster.getCoreMemberById( 1 ).databaseDirectory() );
        fsa.copyRecursively( testDir.databaseDir(), cluster.getCoreMemberById( 2 ).databaseDirectory() );
        cluster.start();

        // then
        dataMatchesEventually( before, cluster.coreMembers() );
    }
}
