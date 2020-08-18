/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.clusteringsupport;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.configuration.SecuritySettings;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.DbRepresentation;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.backup.BackupTestUtil.createBackup;
import static com.neo4j.backup.BackupTestUtil.restoreFromBackup;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStopped;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.stopDatabase;
import static com.neo4j.causalclustering.common.DataMatching.dataMatchesEventually;
import static com.neo4j.configuration.SecuritySettings.NATIVE_REALM_NAME;
import static com.neo4j.security.SecurityHelpers.newUser;
import static com.neo4j.security.SecurityHelpers.userCanLogin;
import static org.apache.commons.lang3.RandomStringUtils.random;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.SettingValueParsers.TRUE;
import static org.neo4j.function.Predicates.await;
import static org.neo4j.graphdb.Label.label;

@ClusterExtension
@TestDirectoryExtension
class ClusterRestoreIT
{
    @Inject
    private ClusterFactory clusterFactory;

    @Inject
    private FileSystemAbstraction fsa;

    @Inject
    private TestDirectory testDirectory;

    private Path backupsDirectory;
    private Cluster cluster;

    @BeforeEach
    void before() throws IOException
    {
        backupsDirectory = testDirectory.cleanDirectoryPath( "backups" );
    }

    @AfterEach
    void after()
    {
        cluster.shutdown();
    }

    private void prepare( ThrowingConsumer<Cluster,Exception> preparation ) throws Exception
    {
        ClusterConfig clusterConfig = ClusterConfig.clusterConfig();

        clusterConfig.withSharedCoreParam( GraphDatabaseSettings.auth_enabled, TRUE );
        clusterConfig.withSharedCoreParam( SecuritySettings.authentication_providers, NATIVE_REALM_NAME );
        clusterConfig.withSharedCoreParam( SecuritySettings.authorization_providers, NATIVE_REALM_NAME );

        Cluster cluster = clusterFactory.createCluster( clusterConfig );
        try
        {
            cluster.start();
            preparation.accept( cluster );
        }
        finally
        {
            cluster.shutdown();
        }
    }

    @Test
    void restoreOnlySystemDatabase() throws Exception
    {
        // given
        prepare( cluster -> backup( cluster, SYSTEM_DATABASE_NAME ) );

        // when
        cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig() );
        restore( cluster );

        cluster.start();

        // then
        assertDatabaseEventuallyStarted( SYSTEM_DATABASE_NAME, cluster );
        dataMatchesEventually( cluster, SYSTEM_DATABASE_NAME );
    }

    @Test
    void restoreSystemAndStoppedDatabases() throws Exception
    {
        // given
        prepare( cluster ->
        {
            createDatabase( "foo", cluster );
            assertDatabaseEventuallyStarted( "foo", cluster );

            backup( cluster, "foo" );

            stopDatabase( "foo", cluster );
            assertDatabaseEventuallyStopped( "foo", cluster );

            backup( cluster, SYSTEM_DATABASE_NAME );
        } );

        // when
        cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig() );
        restore( cluster );

        cluster.start();

        // then
        assertDatabaseEventuallyStarted( SYSTEM_DATABASE_NAME, cluster );
        assertDatabaseEventuallyStopped( "foo", cluster );
    }

    @Test
    void restoreSystemAndStartedDatabases() throws Exception
    {
        // given
        prepare( cluster ->
        {
            createDatabase( "foo", cluster );
            assertDatabaseEventuallyStarted( "foo", cluster );

            backup( cluster, SYSTEM_DATABASE_NAME, "foo" );
        } );

        // when
        cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig() );
        restore( cluster, "foo" );

        cluster.start();

        // then
        assertDatabaseEventuallyStarted( SYSTEM_DATABASE_NAME, cluster );
        assertDatabaseEventuallyStarted( "foo", cluster );
    }

    @Test
    void restoreDatabasesWithData() throws Exception
    {
        // given
        Map<String,DbRepresentation> dbSnapshots = new HashMap<>();

        prepare( cluster ->
        {
            createDatabase( "foo", cluster );
            assertDatabaseEventuallyStarted( "foo", cluster );

            cluster.coreTx( "foo", ( db, tx ) -> randomData( tx ) );
            cluster.coreTx( DEFAULT_DATABASE_NAME, ( db, tx ) -> randomData( tx ) );

            cluster.systemTx( ( db, tx ) -> newUser( tx, "bat", "man" ) );

            backup( cluster, SYSTEM_DATABASE_NAME, DEFAULT_DATABASE_NAME, "foo" );
            populateSnapshots( cluster, dbSnapshots, DEFAULT_DATABASE_NAME, "foo" );
        } );

        // when
        cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig() );
        restore( cluster, DEFAULT_DATABASE_NAME, "foo" );

        cluster.start();

        // then
        assertDatabaseEventuallyStarted( SYSTEM_DATABASE_NAME, cluster );
        assertDatabaseEventuallyStarted( DEFAULT_DATABASE_NAME, cluster );
        assertDatabaseEventuallyStarted( "foo", cluster );

        dataMatchesEventually( cluster, SYSTEM_DATABASE_NAME );
        dataMatchesEventually( dbSnapshots::get, DEFAULT_DATABASE_NAME, cluster.allMembers() );
        dataMatchesEventually( dbSnapshots::get, "foo", cluster.allMembers() );

        for ( ClusterMember member : cluster.allMembers() )
        {
            await( () -> userCanLogin( "bat", "man", member.defaultDatabase() ) );
        }
    }

    private static void randomData( Transaction tx )
    {
        // tokens can't contain null-bytes or backticks
        var label = label( randomAlphanumeric( 20 ) );
        var propertyKey = randomAlphanumeric( 20 );
        var propertyValue = random( 1024 );

        var node = tx.createNode( label );
        node.setProperty( propertyKey, propertyValue );
        tx.commit();
    }

    private static void populateSnapshots( Cluster cluster, Map<String,DbRepresentation> dbRepresentations, String... databaseNames )
            throws TimeoutException
    {
        for ( String databaseName : databaseNames )
        {
            CoreClusterMember leader = cluster.awaitLeader( databaseName );
            dbRepresentations.put( databaseName, DbRepresentation.of( leader.database( databaseName ) ) );
        }
    }

    void backup( Cluster cluster, String... databaseNames ) throws Exception
    {
        for ( String databaseName : databaseNames )
        {
            CoreClusterMember leader = cluster.awaitLeader( databaseName );
            createBackup( leader, backupsDirectory, databaseName );
        }
    }

    void restore( Cluster cluster, String... databaseNames ) throws IOException
    {
        for ( CoreClusterMember core : cluster.coreMembers() )
        {
            Path backupPath = backupsDirectory.resolve( SYSTEM_DATABASE_NAME );
            restoreFromBackup( backupPath, fsa, core, SYSTEM_DATABASE_NAME );
        }

        for ( String databaseName : databaseNames )
        {
            Path backupPath = backupsDirectory.resolve( databaseName );
            for ( ClusterMember member : cluster.allMembers() )
            {
                restoreFromBackup( backupPath, fsa, member, databaseName );
            }
        }
    }
}
