/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.scenarios;

import com.neo4j.backup.BackupTestUtil;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.DataCreator;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.commandline.dbms.UnbindFromClusterCommandProvider;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.restore.RestoreDatabaseCli;
import com.neo4j.test.causalclustering.ClusterConfig;
import com.neo4j.test.causalclustering.ClusterExtension;
import com.neo4j.test.causalclustering.ClusterFactory;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import picocli.CommandLine;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import org.neo4j.cli.ExecutionContext;
import org.neo4j.commandline.dbms.DumpCommand;
import org.neo4j.commandline.dbms.LoadCommand;
import org.neo4j.dbms.archive.Dumper;
import org.neo4j.dbms.archive.Loader;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStarted;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.assertDatabaseEventuallyStopped;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.createDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.dropDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.startDatabase;
import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.stopDatabase;
import static com.neo4j.causalclustering.common.DataMatching.dataMatchesEventually;
import static com.neo4j.configuration.OnlineBackupSettings.online_backup_listen_address;
import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.configuration.Config.DEFAULT_CONFIG_FILE_NAME;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.transaction_logs_root_path;
import static org.neo4j.graphdb.Label.label;

@ClusterExtension
@TestDirectoryExtension
@ExtendWith( SuppressOutputExtension.class )
@ResourceLock( Resources.SYSTEM_OUT )
@Disabled // Disabled for reducing flakiness - this test does not test any new functionality - was developed to test the workaround
class StoppedDatabaseRolledAwayIT
{
    private static final String STOPPED_DATABASE = "foo";

    @Inject
    private ClusterFactory clusterFactory;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory testDirectory;

    @Test
    void minorityRollAway() throws Exception
    {
        var cluster = setup( false );
        roll( 1, cluster );
        startDatabase( STOPPED_DATABASE, cluster );
        checkClusterAfterStartDatabase( cluster );
        cluster.shutdown();
    }

    @ValueSource( booleans = {false, true} )
    @ParameterizedTest( name = "loadNotRestore: {0}" )
    void majorityRollAway( boolean load ) throws Exception
    {
        var cluster = setup( true );
        roll( 2, cluster );
        dropDatabase( STOPPED_DATABASE, cluster );
        restartCluster( cluster, false );
        if ( load )
        {
            loadDatabase( cluster );
        }
        else
        {
            restoreDatabase( cluster );
        }
        createDatabase( STOPPED_DATABASE, cluster );
        checkClusterAfterStartDatabase( cluster );
        cluster.shutdown();
    }

    @ValueSource( booleans = {false, true} )
    @ParameterizedTest( name = "loadNotRestore: {0}" )
    void completeRollAway( boolean load ) throws Exception
    {
        var cluster = setup( true );
        roll( 3, cluster );
        restartCluster( cluster, true );
        if ( load )
        {
            loadDatabase( cluster );
        }
        else
        {
            restoreDatabase( cluster );
        }
        startDatabase( STOPPED_DATABASE, cluster );
        checkClusterAfterStartDatabase( cluster );
        cluster.shutdown();
    }

    private Cluster setup( boolean needSave ) throws Exception
    {
        var cluster = clusterFactory.createCluster( ClusterConfig.clusterConfig()
                .withNumberOfCoreMembers( 3 )
                .withNumberOfReadReplicas( 2 ) );

        cluster.start();
        addData( DEFAULT_DATABASE_NAME, cluster );

        createDatabase( STOPPED_DATABASE, cluster );
        assertDatabaseEventuallyStarted( STOPPED_DATABASE, cluster );
        addData( STOPPED_DATABASE, cluster );

        cluster.awaitLeader( SYSTEM_DATABASE_NAME );
        cluster.awaitLeader();
        cluster.awaitLeader( STOPPED_DATABASE );
        if ( needSave )
        {
            backupDatabase( cluster );
        }
        stopDatabase( STOPPED_DATABASE, cluster );
        assertDatabaseEventuallyStopped( STOPPED_DATABASE, cluster );
        if ( needSave )
        {
            dumpDatabase( cluster );
        }
        return cluster;
    }

    private void roll( int count, Cluster cluster ) throws Exception
    {
        for ( var i = 0; i < count; i++ )
        {
            // add a new member, add some data, check it is available on all nodes
            var newMember = cluster.addCoreMemberWithId( i + 3 );
            newMember.start();
            // remove an old member
            cluster.removeCoreMemberWithServerId( i );
            cluster.awaitLeader( SYSTEM_DATABASE_NAME );
            cluster.awaitLeader();
        }
    }

    private void restartCluster( Cluster cluster, boolean unbind ) throws InterruptedException, ExecutionException, TimeoutException
    {
        fixDiscoveryAddresses( cluster );
        cluster.shutdownReadReplicas();
        cluster.startReadReplicas();
        cluster.shutdownCoreMembers();

        if ( unbind )
        {
            cluster.coreMembers().forEach( this::unbind );
        }

        cluster.readReplicas().forEach( rr -> assertEquals( 1, DataCreator.countNodes( rr ) ) );

        cluster.startCoreMembers();
        cluster.awaitLeader( SYSTEM_DATABASE_NAME );
        cluster.awaitLeader();
    }

    private void checkClusterAfterStartDatabase( Cluster cluster ) throws Exception
    {
        assertDatabaseEventuallyStarted( STOPPED_DATABASE, cluster );

        cluster.awaitLeader( SYSTEM_DATABASE_NAME );

        addData( STOPPED_DATABASE, cluster );
        assertEquals( 2, DataCreator.countNodes( cluster.awaitLeader( STOPPED_DATABASE ), STOPPED_DATABASE ) );
        addData( "neo4j", cluster );
        assertEquals( 2, DataCreator.countNodes( cluster.awaitLeader() ) );
    }

    private void fixDiscoveryAddresses( Cluster cluster )
    {
        var newDiscoveryAddresses = cluster.coreMembers().stream().map( CoreClusterMember::config )
                .map( config -> config.get( CausalClusteringSettings.discovery_advertised_address ) ).collect( Collectors.toList() );
        cluster.coreMembers().forEach( member -> member.config().set( CausalClusteringSettings.initial_discovery_members, newDiscoveryAddresses ) );
        cluster.readReplicas().forEach( member -> member.config().set( CausalClusteringSettings.initial_discovery_members, newDiscoveryAddresses ) );
    }

    private void dumpDatabase( Cluster cluster ) throws Exception
    {
        var member = cluster.randomCoreMember( true ).orElseThrow();
        var dump = testDirectory.homePath().resolve( "foo.dump" );
        fs.deleteRecursively( dump.toFile() );

        var context = getExtensionContext( member );
        var command = new DumpCommand( context, new Dumper( context.err() ) );

        String[] args = {"--database=foo", "--to=" + dump.toAbsolutePath()};
        CommandLine.populateCommand( command, args );

        command.execute();
    }

    private void loadDatabase( Cluster cluster )
    {
        var dump = testDirectory.homePath().resolve( "foo.dump" );
        cluster.coreMembers().forEach( member ->
        {
            try
            {
                var context = getExtensionContext( member );
                var command = new LoadCommand( context, new Loader( context.err() ) );

                String[] args = {"--database=foo", "--from=" + dump.toAbsolutePath(), "--force"};
                CommandLine.populateCommand( command, args );

                command.execute();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        } );
    }

    private void backupDatabase( Cluster cluster ) throws Exception
    {
        var member = cluster.randomCoreMember( true ).orElseThrow();
        var backups = testDirectory.homePath().resolve( "backups" );
        fs.deleteRecursively( backups.toFile() );
        fs.mkdirs( backups.toFile() );
        var address = member.config().get( online_backup_listen_address ).toString();

        var neo4jHome = member.homePath();
        String[] args = {"--database=" + STOPPED_DATABASE, "--from", address, "--backup-dir=" + backups.toAbsolutePath()};
        BackupTestUtil.runBackupToolFromSameJvm( neo4jHome, args );
    }

    private void restoreDatabase( Cluster cluster )
    {
        var backup = testDirectory.homePath().resolve( "backups/foo" );
        cluster.coreMembers().forEach( member ->
        {
            try
            {
                var command = new RestoreDatabaseCli( getExtensionContext( member ) );

                String[] args = {"--database=" + STOPPED_DATABASE, "--from=" + backup.toAbsolutePath(), "--force"};
                CommandLine.populateCommand( command, args );

                command.execute();
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
        } );
    }

    private void addData( String databaseName, Cluster cluster ) throws Exception
    {
        cluster.coreTx( databaseName, ( db, tx ) ->
        {
            Node node = tx.createNode( label( "boo" ) );
            node.setProperty( "id", UUID.randomUUID().toString() );
            tx.commit();
        } );
        dataMatchesEventually( cluster, databaseName );
    }

    private void unbind( CoreClusterMember member )
    {
        var neo4jHome = member.homePath();
        var configDir = member.homePath().resolve( "configDir" );

        var context = new ExecutionContext( neo4jHome, configDir, System.out, System.err, fs );
        var command = new UnbindFromClusterCommandProvider().createCommand( context );

        CommandLine.populateCommand( command );
        command.execute();
    }

    private ExecutionContext getExtensionContext( CoreClusterMember member ) throws IOException
    {
        var dataDir = member.neo4jLayout().databasesDirectory();
        var trxDir = member.neo4jLayout().transactionLogsRootDirectory();
        var neo4jHome = member.homePath();
        var configDir = testDirectory.directoryPath( "configDir" );
        appendConfigSetting( configDir, databases_root_path, dataDir );
        appendConfigSetting( configDir, transaction_logs_root_path, trxDir );

        return new ExecutionContext( neo4jHome, configDir, System.out, System.err, fs );
    }

    private <T> void appendConfigSetting( Path configDir, Setting<T> setting, T value ) throws IOException
    {
        Path configFile = configDir.resolve( DEFAULT_CONFIG_FILE_NAME );
        List<String> allSettings;
        if ( fs.fileExists( configFile.toFile() ) )
        {
            allSettings = Files.readAllLines( configFile );
        }
        else
        {
            allSettings = new ArrayList<>();
        }
        allSettings.add( formatProperty( setting, value ) );
        Files.write( configFile, allSettings );
    }

    private <T> String formatProperty( Setting<T> setting, T value )
    {
        String valueString = value.toString();
        if ( value instanceof Path )
        {
            valueString = value.toString().replace( '\\', '/' );
        }
        return format( "%s=%s", setting.name(), valueString );
    }
}
