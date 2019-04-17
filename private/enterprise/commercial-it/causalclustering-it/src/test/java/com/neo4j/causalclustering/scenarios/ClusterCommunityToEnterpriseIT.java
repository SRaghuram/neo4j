/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.discovery.IpFamily;
import com.neo4j.causalclustering.discovery.SharedDiscoveryServiceFactory;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.DatabaseManagementServiceBuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static com.neo4j.causalclustering.common.Cluster.dataMatchesEventually;
import static java.util.Collections.emptyMap;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class ClusterCommunityToEnterpriseIT
{
    private Cluster cluster;
    private FileSystemAbstraction fsa;

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();
    @Rule
    public DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();

    @Before
    public void setup()
    {
        fsa = fileSystemRule.get();

        cluster = new Cluster( testDir.directory( "cluster" ), 3, 0,
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
        DatabaseManagementService managementService = new DatabaseManagementServiceBuilder().newEmbeddedDatabaseBuilder( testDir.storeDir() )
                .setConfig( GraphDatabaseSettings.allow_upgrade, Settings.TRUE )
                .setConfig( GraphDatabaseSettings.record_format, HighLimit.NAME )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Boolean.FALSE.toString() ).newDatabaseManagementService();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        DatabaseLayout databaseLayout = ((GraphDatabaseAPI) database).databaseLayout();
        managementService.shutdown();
        Config config = Config.builder().withSetting( OnlineBackupSettings.online_backup_enabled, Settings.FALSE ).withSetting(
                GraphDatabaseSettings.transaction_logs_root_path, databaseLayout.getTransactionLogsDirectory().getParentFile().getAbsolutePath() ).build();
        DbRepresentation before = DbRepresentation.of( testDir.storeDir(), config );

        // when
        copyStoreToCore( databaseLayout, 0 );
        copyStoreToCore( databaseLayout, 1 );
        copyStoreToCore( databaseLayout, 2 );
        cluster.start();

        // then
        dataMatchesEventually( before, cluster.coreMembers() );
    }

    private void copyStoreToCore( DatabaseLayout databaseLayout, int i ) throws IOException
    {
        DatabaseLayout coreLayout = cluster.getCoreMemberById( i ).databaseLayout();
        fsa.copyRecursively( databaseLayout.databaseDirectory(), coreLayout.databaseDirectory() );
        fsa.copyRecursively( databaseLayout.getTransactionLogsDirectory(), coreLayout.getTransactionLogsDirectory() );
    }
}
