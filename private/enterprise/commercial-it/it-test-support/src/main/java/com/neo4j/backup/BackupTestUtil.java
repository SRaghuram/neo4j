/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.restore.RestoreDatabaseCommand;
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

    public static void restoreFromBackup( File backup, FileSystemAbstraction fsa, ClusterMember clusterMember ) throws IOException, CommandFailed
    {
        restoreFromBackup( backup, fsa, clusterMember, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    }

    public static void restoreFromBackup( File backup, FileSystemAbstraction fsa,
            ClusterMember clusterMember, String databaseName ) throws IOException, CommandFailed
    {
        Config config = Config.fromSettings( clusterMember.config().getRaw() )
                .withSetting( GraphDatabaseSettings.default_database, databaseName )
                .withConnectorsDisabled()
                .build();
        DatabaseIdRepository databaseIdRepository = new TestDatabaseIdRepository();
        RestoreDatabaseCommand restoreDatabaseCommand = new RestoreDatabaseCommand( fsa, backup, config, databaseIdRepository.get( databaseName ), true );
        restoreDatabaseCommand.execute();
    }

    public static GraphDatabaseFacade createSomeData( Cluster cluster ) throws Exception
    {
        return cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } ).defaultDatabase();
    }

    public static String[] backupArguments( String from, File backupsDir, DatabaseId databaseId )
    {
        return new String[]{
                "--from=" + from,
                "--cc-report-dir=" + backupsDir,
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
        try ( ExitCodeMemorizingOutsideWorld outsideWorld = new ExitCodeMemorizingOutsideWorld() )
        {
            AdminTool adminTool = new AdminTool( CommandLocator.fromServiceLocator(), outsideWorld, false );

            List<String> allArgs = new ArrayList<>();
            allArgs.add( "backup" );
            Collections.addAll( allArgs, args );

            Path homeDir = neo4jHome.getAbsoluteFile().toPath();
            Path configDir = homeDir.resolve( "conf" );
            adminTool.execute( homeDir, configDir, allArgs.toArray( new String[0] ) );

            return outsideWorld.getExitCode();
        }
    }
}
