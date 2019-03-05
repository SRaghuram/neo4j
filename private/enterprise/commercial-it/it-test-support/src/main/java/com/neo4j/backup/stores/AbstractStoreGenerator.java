/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.stores;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;

import static com.neo4j.backup.BackupTestUtil.createBackupFromCore;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public abstract class AbstractStoreGenerator implements BackupStore
{
    abstract CoreClusterMember createData( Cluster cluster ) throws Exception;

    abstract void modify( File backup ) throws Exception;

    @Override
    public Optional<DefaultDatabasesBackup> generate( File baseBackupDir, Cluster backupCluster ) throws Exception
    {
        CoreClusterMember core = createData( backupCluster );
        File defaultBackupFromCore = createBackupFromCore( core, backupDir( baseBackupDir, DEFAULT_DATABASE_NAME ), DEFAULT_DATABASE_NAME );
        File systemBackupFromCore = createBackupFromCore( core, backupDir( baseBackupDir, SYSTEM_DATABASE_NAME ), SYSTEM_DATABASE_NAME );
        DefaultDatabasesBackup backups = new DefaultDatabasesBackup( defaultBackupFromCore, systemBackupFromCore );
        modify( defaultBackupFromCore );
        return Optional.of( backups );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    static void deleteTransactionLogs( File dir ) throws IOException
    {
        File[] txLogs = dir.listFiles( TransactionLogFiles.DEFAULT_FILENAME_FILTER );
        if ( txLogs == null )
        {
            throw new IllegalStateException( "No transaction logs found in " + dir + " containing: " + Arrays.toString( dir.list() ) );
        }
        for ( File transaction : txLogs )
        {
            Files.delete( transaction.toPath() );
        }
    }

    private static File backupDir( File baseDir, String database ) throws IOException
    {
        File dir = new File( baseDir, database + "-backup-" + UUID.randomUUID() );
        Files.createDirectories( dir.toPath() );
        return dir;
    }
}
