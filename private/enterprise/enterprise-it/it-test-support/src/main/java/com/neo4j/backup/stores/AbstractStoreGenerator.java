/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.stores;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.UUID;

import static com.neo4j.backup.BackupTestUtil.createBackup;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

public abstract class AbstractStoreGenerator implements BackupStore
{
    abstract CoreClusterMember createData( Cluster cluster ) throws Exception;

    abstract void modify( Path backup ) throws Exception;

    @Override
    public Optional<DefaultDatabasesBackup> generate( Path baseBackupDir, Cluster backupCluster ) throws Exception
    {
        CoreClusterMember core = createData( backupCluster );
        Path defaultBackupFromCore = createBackup( core, backupDir( baseBackupDir, DEFAULT_DATABASE_NAME ), DEFAULT_DATABASE_NAME );
        Path systemBackupFromCore = createBackup( core, backupDir( baseBackupDir, SYSTEM_DATABASE_NAME ), SYSTEM_DATABASE_NAME );
        DefaultDatabasesBackup backups = new DefaultDatabasesBackup( defaultBackupFromCore, systemBackupFromCore );
        modify( defaultBackupFromCore );
        return Optional.of( backups );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName();
    }

    private static Path backupDir( Path baseDir, String database ) throws IOException
    {
        Path dir = baseDir.resolve( database + "-backup-" + UUID.randomUUID() );
        Files.createDirectories( dir );
        return dir;
    }
}
