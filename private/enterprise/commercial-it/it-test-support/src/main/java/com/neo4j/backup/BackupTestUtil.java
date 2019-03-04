/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.CoreGraphDatabase;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.neo4j.commandline.admin.AdminTool;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.commandline.admin.CommandLocator;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.restore.RestoreDatabaseCommand;
import org.neo4j.test.ProcessStreamHandler;
import org.neo4j.test.StreamConsumer;
import org.neo4j.test.ports.PortAuthority;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.proc.ProcessUtil.getClassPath;
import static org.neo4j.test.proc.ProcessUtil.getJavaExecutable;

public class BackupTestUtil
{
    private static String backupAddress( CoreClusterMember core )
    {
        return core.settingValue( "causal_clustering.transaction_listen_address" );
    }

    public static File createBackupFromCore( CoreClusterMember core, String backupName, File baseBackupDir, String database ) throws Exception
    {
        String[] args = backupArguments( backupAddress( core ), baseBackupDir, backupName, database );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( baseBackupDir, args ) );
        return new File( baseBackupDir, backupName );
    }

    public static void restoreFromBackup( File backup, FileSystemAbstraction fsa, ClusterMember<?> clusterMember ) throws IOException, CommandFailed
    {
        restoreFromBackup( backup, fsa, clusterMember, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    }

    public static void restoreFromBackup( File backup, FileSystemAbstraction fsa,
            ClusterMember<?> clusterMember, String database ) throws IOException, CommandFailed
    {
        Config config = Config.fromSettings( clusterMember.config().getRaw() )
                .withSetting( GraphDatabaseSettings.active_database, database )
                .withConnectorsDisabled()
                .build();
        RestoreDatabaseCommand restoreDatabaseCommand = new RestoreDatabaseCommand( fsa, backup, config, database, true );
        restoreDatabaseCommand.execute();
    }

    public static CoreGraphDatabase createSomeData( Cluster cluster ) throws Exception
    {
        return cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } ).database();
    }

    public static String[] backupArguments( String from, File backupsDir, String name )
    {
        return backupArguments( from, backupsDir, name, null );
    }

    public static String[] backupArguments( String from, File backupsDir, String name, String database )
    {
        List<String> args = new ArrayList<>();
        args.add( "--from=" + from );
        args.add( "--cc-report-dir=" + backupsDir );
        args.add( "--backup-dir=" + backupsDir );
        if ( database != null )
        {
            args.add( "--database=" + database );
        }
        args.add( "--name=" + name );
        return args.toArray( new String[0] );

    }

    public static Config getConfig()
    {
        Map<String, String> config = MapUtil.stringMap(
                GraphDatabaseSettings.record_format.name(), Standard.LATEST_NAME,
                OnlineBackupSettings.online_backup_listen_address.name(), "127.0.0.1:" + PortAuthority.allocatePort()
        );

        return Config.defaults( config );
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
