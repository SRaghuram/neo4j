/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.clusteringsupport;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.common.ClusterMember;
import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.CoreGraphDatabase;
import com.neo4j.causalclustering.core.consensus.roles.Role;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.restore.RestoreDatabaseCommand;

import static com.neo4j.util.TestHelpers.runBackupToolFromOtherJvmToGetExitCode;
import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.Label.label;

public class BackupUtil
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

    public static void restoreFromBackup( File backup, FileSystemAbstraction fsa, ClusterMember clusterMember ) throws IOException, CommandFailed
    {
        restoreFromBackup( backup, fsa, clusterMember, GraphDatabaseSettings.DEFAULT_DATABASE_NAME );
    }

    public static void restoreFromBackup( File backup, FileSystemAbstraction fsa,
            ClusterMember clusterMember, String database ) throws IOException, CommandFailed
    {
        Config config = Config.fromSettings( clusterMember.config().getRaw() )
                .withSetting( GraphDatabaseSettings.active_database, database )
                .withConnectorsDisabled()
                .build();
        RestoreDatabaseCommand restoreDatabaseCommand = new RestoreDatabaseCommand( fsa, backup, config, database, true );
        restoreDatabaseCommand.execute();
    }

    public static CoreGraphDatabase createSomeData( Cluster<?> cluster ) throws Exception
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
}
