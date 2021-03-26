/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup;

import com.neo4j.backup.impl.OnlineBackupCommand;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.restore.RestoreDatabaseCommand;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;

import org.neo4j.cli.AdminTool;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.graphdb.Node;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.StreamConsumer;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.backupAddress;
import static org.apache.commons.lang3.ArrayUtils.addAll;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.proc.ProcessUtil.start;

public final class BackupTestUtil
{
    private BackupTestUtil()
    {
    }

    public static Path createBackup( ClusterMember member, Path baseBackupDir, String databaseName ) throws Exception
    {
        String[] args = backupArguments( backupAddress( member.defaultDatabase() ), baseBackupDir, databaseName );
        assertThat( runBackupToolFromOtherJvmToGetExitCode( baseBackupDir, args ), equalTo( 0 ) );
        return baseBackupDir.resolve( databaseName );
    }

    public static void restoreFromBackup( Path fromDatabasePath, FileSystemAbstraction fsa, ClusterMember clusterMember, String databaseName )
            throws IOException
    {
        Config config = Config.newBuilder().fromConfig( clusterMember.config() ).build();
        ConfigUtils.disableAllConnectors( config );
        final var clusterStateLayout = ClusterStateLayout.of( config.get( CausalClusteringSettings.cluster_state_directory ) );
        final var raftGroupDirectory = clusterStateLayout.raftGroupDir( databaseName );
        final var databaseLayout = Neo4jLayout.of( config ).databaseLayout( databaseName );
        RestoreDatabaseCommand restoreDatabaseCommand =
                new RestoreDatabaseCommand( fsa, new PrintStream( System.out ), fromDatabasePath, databaseLayout, raftGroupDirectory, true, false );
        restoreDatabaseCommand.execute();
    }

    public static GraphDatabaseFacade createSomeData( Cluster cluster ) throws Exception
    {
        return cluster.primaryTx( ( db, tx ) ->
        {
            Node node = tx.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.commit();
        } ).defaultDatabase();
    }

    public static String[] backupArguments( String from, Path backupsDir, String databaseName )
    {
        return new String[]{
                "--from=" + from,
                "--report-dir=" + backupsDir,
                "--backup-dir=" + backupsDir,
                "--database=" + databaseName
        };
    }

    public static int runBackupToolFromOtherJvmToGetExitCode( Path neo4jHome, String... args ) throws Exception
    {
        return runBackupToolFromOtherJvmToGetExitCode( neo4jHome, System.out, System.err, false, args );
    }

    public static int runBackupToolFromOtherJvmToGetExitCode( Path neo4jHome, PrintStream outPrintStream, PrintStream errPrintStream,
            boolean debug, String... args ) throws Exception
    {
        var arguments = addAll( new String[]{AdminTool.class.getName(), "backup"}, args );
        var process = start( builder ->
        {
            builder.environment().put( "NEO4J_HOME", neo4jHome.toAbsolutePath().toString() );
            builder.environment().put( "NEO4J_CONF", neo4jHome.toAbsolutePath().toString() );
            if ( debug )
            {
                builder.environment().put( "NEO4J_DEBUG", "anything_works" );
            }
        }, arguments );

        ProcessStreamHandler processStreamHandler =
                new ProcessStreamHandler( process, false, "", StreamConsumer.IGNORE_FAILURES, outPrintStream, errPrintStream );
        return processStreamHandler.waitForResult();
    }

    public static int runBackupToolFromSameJvm( Path neo4jHome, String... args )
    {
        final var homeDir = neo4jHome.toAbsolutePath();
        final var configDir = homeDir.resolve( "conf" );
        final var ctx = new ExecutionContext( homeDir, configDir );

        final var command = CommandLine.populateCommand( new OnlineBackupCommand( ctx ), args );

        try
        {
            return command.call();
        }
        catch ( Exception e )
        {
            throw new RuntimeException( e );
        }
    }
}
