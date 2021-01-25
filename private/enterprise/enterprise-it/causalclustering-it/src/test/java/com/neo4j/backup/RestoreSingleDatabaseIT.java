/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.restore.RestoreDatabaseCommand;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import com.neo4j.test.driver.DriverExtension;
import com.neo4j.test.driver.DriverFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.neo4j.configuration.Config;
import org.neo4j.driver.Driver;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyDoesNotExist;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.DataMatching.dataMatchesEventually;
import static com.neo4j.configuration.OnlineBackupSettings.online_backup_listen_address;
import static com.neo4j.test.causalclustering.ClusterConfig.clusterConfig;
import static com.neo4j.test.driver.DriverTestHelper.dropDatabaseWait;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.conditions.Conditions.TRUE;

@TestDirectoryExtension
@ClusterExtension
@DriverExtension
@ResourceLock( Resources.SYSTEM_OUT )
@ExtendWith( SuppressOutputExtension.class )
public class RestoreSingleDatabaseIT
{
    @Inject
    private static ClusterFactory clusterFactory;

    @Inject
    private static FileSystemAbstraction fs;

    @Inject
    private static TestDirectory testDirectory;

    @Inject
    private static DriverFactory driverFactory;

    private static Path backupsDirectory;
    private static Cluster cluster;
    private static Driver driver;

    @BeforeAll
    static void before() throws IOException, ExecutionException, InterruptedException
    {
        backupsDirectory = testDirectory.cleanDirectory( "backups" );
        cluster = clusterFactory.createCluster( clusterConfig() );
        cluster.start();
        driver = driverFactory.graphDatabaseDriver( cluster );
    }

    @ParameterizedTest( name = "databaseName = {0}" )
    @ValueSource( strings = {DEFAULT_DATABASE_NAME, "foo"} )
    void shouldRestoreSingleDatabaseInRunningCluster( String databaseName ) throws Exception
    {
        // given: a cluster with a database that has some data in it
        if ( !DEFAULT_DATABASE_NAME.equals( databaseName ) )
        {
            createDatabase( databaseName, cluster );
            assertDatabaseEventuallyStarted( databaseName, cluster );
        }

        cluster.coreTx( databaseName, ( db, tx ) ->
        {
            Node person = tx.createNode( Label.label( "Person" ) );
            person.setProperty( "name", "less" );
            tx.commit();
        } );

        dataMatchesEventually( cluster, databaseName );

        // and a backup of that database from any of the members
        var backupServer = cluster.randomMember( true ).orElseThrow();
        int exitCode = BackupTestUtil
                .runBackupToolFromSameJvm( backupsDirectory,
                        "--from", backupServer.settingValue( online_backup_listen_address ).toString(),
                        "--database", databaseName,
                        "--backup-dir", backupsDirectory.toString() );

        assertEquals( 0, exitCode );

        // and the database has been dropped
        dropDatabaseWait( driver, databaseName );
        assertDatabaseEventuallyDoesNotExist( databaseName, cluster );

        // when: restoring the database backup to every instance (neo4j-admin restore)
        for ( ClusterMember member : cluster.allMembers() )
        {
            runRestore( fs, backupsDirectory, member.config(), databaseName );
        }

        // and creating the database
        createDatabase( databaseName, cluster );

        // then: the database should start everywhere
        assertDatabaseEventuallyStarted( databaseName, cluster );

        // and contain the data which was previously in it
        cluster.coreTx( databaseName, ( db, tx ) -> assertNotNull( tx.findNode( Label.label( "Person" ), "name", "less" ) ) );

        // on every server
        dataMatchesEventually( cluster, databaseName );
    }

    private static void runRestore( FileSystemAbstraction fs, Path backupLocation, Config memberConfig, String databaseName ) throws Exception
    {
        Config restoreCommandConfig = Config.newBuilder().fromConfig( memberConfig ).build();
        final var clusterStateLayout = ClusterStateLayout.of( restoreCommandConfig.get( CausalClusteringSettings.cluster_state_directory ) );
        final var raftGroupDirectory = clusterStateLayout.raftGroupDir( databaseName );
        final var databaseLayout = Neo4jLayout.of( restoreCommandConfig ).databaseLayout( databaseName );
        new RestoreDatabaseCommand( fs, new PrintStream( System.out ), backupLocation.resolve( databaseName ), databaseLayout, raftGroupDirectory,
                                    true, false ).execute();
    }
}
