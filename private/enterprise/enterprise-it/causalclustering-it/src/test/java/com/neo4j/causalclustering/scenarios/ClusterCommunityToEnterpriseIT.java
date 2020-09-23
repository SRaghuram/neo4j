/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.kernel.impl.store.format.highlimit.HighLimit;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.io.IOException;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.DbmsExtension;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.common.DataMatching.dataMatchesEventually;
import static com.neo4j.configuration.OnlineBackupSettings.online_backup_enabled;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_METHOD;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.allow_upgrade;
import static org.neo4j.configuration.GraphDatabaseSettings.record_format;

@TestDirectoryExtension
@ClusterExtension
@TestInstance( PER_METHOD )
@DbmsExtension( configurationCallback = "configure" )
class ClusterCommunityToEnterpriseIT
{
    @Inject
    private TestDirectory testDir;
    @Inject
    private ClusterFactory clusterFactory;
    @Inject
    private DatabaseManagementService managementService;

    @ExtensionCallback
    void configure( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setDatabaseRootDirectory( testDir.directory( "standalone" ) );
        builder.setConfig( allow_upgrade, true )
                .setConfig( record_format, HighLimit.NAME )
                .setConfig( online_backup_enabled, false );
    }

    @Test
    void shouldRestoreBySeedingAllMembers() throws Throwable
    {
        // given
        var clusterConfig = clusterConfig().withRecordFormat( HighLimit.NAME ).withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 );
        var cluster = clusterFactory.createCluster( clusterConfig );
        var database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        var databaseLayout = database.databaseLayout();
        managementService.shutdown();
        var config = Config.newBuilder()
                .set( online_backup_enabled, false )
                .build();
        var before = DbRepresentation.of( databaseLayout, config );

        // when
        for ( var core : cluster.coreMembers() )
        {
            copyStoreToCore( databaseLayout, core );
        }
        cluster.start();

        // then
        dataMatchesEventually( before, DEFAULT_DATABASE_NAME, cluster.coreMembers() );
    }

    private void copyStoreToCore( DatabaseLayout databaseLayout, ClusterMember member ) throws IOException
    {
        var coreLayout = member.databaseLayout();
        testDir.getFileSystem().copyRecursively( databaseLayout.databaseDirectory(), coreLayout.databaseDirectory() );
        testDir.getFileSystem()
               .copyRecursively( databaseLayout.getTransactionLogsDirectory(), coreLayout.getTransactionLogsDirectory() );
    }
}
