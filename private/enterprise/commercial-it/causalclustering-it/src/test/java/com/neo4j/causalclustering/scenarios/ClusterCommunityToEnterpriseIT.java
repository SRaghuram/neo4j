/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.discovery.IpFamily;
import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static com.neo4j.causalclustering.common.Cluster.dataMatchesEventually;
import static com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_enabled;
import static java.util.Collections.emptyMap;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;

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
                new AkkaDiscoveryServiceFactory(), emptyMap(), emptyMap(), emptyMap(), emptyMap(), HighLimit.NAME,
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
        DatabaseManagementService managementService = new DatabaseManagementServiceBuilder( testDir.storeDir() )
                .setConfig( allow_upgrade, true )
                .setConfig( record_format, HighLimit.NAME )
                .setConfig( online_backup_enabled, false )
                .build();
        GraphDatabaseAPI database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        DatabaseLayout databaseLayout = database.databaseLayout();
        managementService.shutdown();
        Config config = Config.newBuilder()
                .set( online_backup_enabled, false )
                .set( transaction_logs_root_path, databaseLayout.getTransactionLogsDirectory().getParentFile().toPath().toAbsolutePath() )
                .build();
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
