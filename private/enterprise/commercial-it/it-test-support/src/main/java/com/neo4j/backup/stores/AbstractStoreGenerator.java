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
import java.util.Optional;
import java.util.UUID;

import org.neo4j.kernel.database.DatabaseId;

import static com.neo4j.backup.BackupTestUtil.createBackupFromCore;

public abstract class AbstractStoreGenerator implements BackupStore
{
    abstract CoreClusterMember createData( Cluster cluster ) throws Exception;

    abstract void modify( File backup ) throws Exception;

    @Override
    public Optional<DefaultDatabasesBackup> generate( File baseBackupDir, Cluster backupCluster ) throws Exception
    {
        CoreClusterMember core = createData( backupCluster );
        DatabaseId defaultDatabaseId = core.databaseIdRepository().defaultDatabase();
        File defaultBackupFromCore = createBackupFromCore( core, backupDir( baseBackupDir, defaultDatabaseId.name() ), defaultDatabaseId );
        DatabaseId systemDatabaseId = core.databaseIdRepository().systemDatabase();
        File systemBackupFromCore = createBackupFromCore( core, backupDir( baseBackupDir, systemDatabaseId.name() ), systemDatabaseId );
        DefaultDatabasesBackup backups = new DefaultDatabasesBackup( defaultBackupFromCore, systemBackupFromCore );
        modify( defaultBackupFromCore );
        return Optional.of( backups );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    private static File backupDir( File baseDir, String database ) throws IOException
    {
        File dir = new File( baseDir, database + "-backup-" + UUID.randomUUID() );
        Files.createDirectories( dir.toPath() );
        return dir;
    }
}
