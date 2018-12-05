/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering;

import java.io.File;
import java.io.IOException;

import org.neo4j.causalclustering.common.ClusterMember;
import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.commandline.admin.CommandFailed;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.restore.RestoreDatabaseCommand;

import static com.neo4j.util.TestHelpers.runBackupToolFromOtherJvmToGetExitCode;
import static org.junit.Assert.assertEquals;
import static org.neo4j.causalclustering.BackupCoreIT.backupArguments;

public class BackupUtil
{
    private static String backupAddress( CoreClusterMember core )
    {
        return core.settingValue( "causal_clustering.transaction_listen_address" );
    }

    public static File createBackupFromCore( CoreClusterMember core, String backupName, File baseBackupDir ) throws Exception
    {
        String[] args = backupArguments( backupAddress( core ), baseBackupDir, backupName );
        assertEquals( 0, runBackupToolFromOtherJvmToGetExitCode( baseBackupDir, args ) );
        return new File( baseBackupDir, backupName );
    }

    public static void restoreFromBackup( File backup, FileSystemAbstraction fsa, ClusterMember clusterMember ) throws IOException, CommandFailed
    {
        Config config = clusterMember.config();
        RestoreDatabaseCommand restoreDatabaseCommand = new RestoreDatabaseCommand( fsa, backup, config, GraphDatabaseSettings.DEFAULT_DATABASE_NAME, true );
        restoreDatabaseCommand.execute();
    }
}
