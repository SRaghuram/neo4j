/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup;

import com.neo4j.backup.impl.OnlineBackupCommand;
import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.restore.RestoreDatabaseCommand;
import picocli.CommandLine;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.cli.AdminTool;
import org.neo4j.cli.ExecutionContext;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.StreamConsumer;

import static com.neo4j.causalclustering.common.CausalClusteringTestHelpers.backupAddress;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.proc.ProcessUtil.getClassPath;
import static org.neo4j.test.proc.ProcessUtil.getJavaExecutable;

public class BackupTestUtil
{
    public static File createBackupFromCore( CoreClusterMember core, File baseBackupDir, DatabaseId databaseId ) throws Exception
    {
        String[] args = backupArguments( backupAddress( core.defaultDatabase() ), baseBackupDir, databaseId );
        assertThat( runBackupToolFromOtherJvmToGetExitCode( baseBackupDir, args ), equalTo( 0 ) );
        return new File( baseBackupDir, databaseId.name() );
    }

    public static void restoreFromBackup( File backup, FileSystemAbstraction fsa, ClusterMember clusterMember ) throws IOException
    {
        restoreFromBackup( backup, fsa, clusterMember, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    }

    public static void restoreFromBackup( File backup, FileSystemAbstraction fsa,
            ClusterMember clusterMember, String databaseName ) throws IOException
    {
        Config config = Config.newBuilder().fromConfig( clusterMember.config() )
                .build();
        ConfigUtils.disableAllConnectors( config );
        RestoreDatabaseCommand restoreDatabaseCommand = new RestoreDatabaseCommand( fsa, backup, config, databaseName, true );
        restoreDatabaseCommand.execute();
    }

    public static GraphDatabaseFacade createSomeData( Cluster cluster ) throws Exception
    {
        return cluster.coreTx( ( db, tx ) ->
        {
            Node node = tx.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.commit();
        } ).defaultDatabase();
    }

    public static String[] backupArguments( String from, File backupsDir, DatabaseId databaseId )
    {
        return new String[]{
                "--from=" + from,
                "--report-dir=" + backupsDir,
                "--backup-dir=" + backupsDir,
                "--database=" + databaseId.name()
        };
    }

    public static int runBackupToolFromOtherJvmToGetExitCode( File neo4jHome, String... args ) throws Exception
    {
        return runBackupToolFromOtherJvmToGetExitCode( neo4jHome, System.out, System.err, false, args );
    }

    public static int runBackupToolFromOtherJvmToGetExitCode( File neo4jHome, PrintStream outPrintStream, PrintStream errPrintStream,
            boolean debug, String... args ) throws Exception
    {
        List<String> allArgs =
                new ArrayList<>( Arrays.asList( getJavaExecutable().toString(), "-cp", getClassPath(), AdminTool.class.getName() ) );
        allArgs.add( "backup" );
        allArgs.addAll( Arrays.asList( args ) );

        ProcessBuilder processBuilder = new ProcessBuilder().command( allArgs.toArray( new String[0] ) );
        processBuilder.environment().put( "NEO4J_HOME", neo4jHome.getAbsolutePath() );
        processBuilder.environment().put( "NEO4J_CONF", neo4jHome.getAbsolutePath() );
        if ( debug )
        {
            processBuilder.environment().put( "NEO4J_DEBUG", "anything_works" );
        }
        Process process = processBuilder.start();
        ProcessStreamHandler processStreamHandler =
                new ProcessStreamHandler( process, false, "", StreamConsumer.IGNORE_FAILURES, outPrintStream, errPrintStream );
        return processStreamHandler.waitForResult();
    }

    public static int runBackupToolFromSameJvm( File neo4jHome, String... args )
    {
        final var homeDir = neo4jHome.getAbsoluteFile().toPath();
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
