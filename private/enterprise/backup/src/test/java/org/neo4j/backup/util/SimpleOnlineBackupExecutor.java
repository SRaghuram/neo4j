/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.util;

import java.nio.file.Path;

import org.neo4j.backup.impl.BackupExecutionException;
import org.neo4j.backup.impl.ConsistencyCheckExecutionException;
import org.neo4j.backup.impl.OnlineBackupContext;
import org.neo4j.backup.impl.OnlineBackupExecutor;
import org.neo4j.helpers.SocketAddress;

public final class SimpleOnlineBackupExecutor
{
    private static final String DEFAULT_BACKUP_NAME = "graph.db";
    private static final String DEFAULT_BACKUP_HOST = "localhost";

    private SimpleOnlineBackupExecutor()
    {
    }

    public static Path executeBackup( int port, Path backupDir )
            throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        return executeBackup( new SocketAddress( DEFAULT_BACKUP_HOST, port ), backupDir, DEFAULT_BACKUP_NAME );
    }

    public static Path executeBackup( SocketAddress address, Path backupDir, String backupName )
            throws BackupExecutionException, ConsistencyCheckExecutionException
    {
        OnlineBackupContext context = OnlineBackupContext.builder()
                .withHostnamePort( address.getHostname(), address.getPort() )
                .withBackupName( backupName )
                .withBackupDirectory( backupDir )
                .withReportsDirectory( backupDir )
                .withConsistencyCheck( true )
                .withConsistencyCheckPropertyOwners( true )
                .build();

        OnlineBackupExecutor executor = OnlineBackupExecutor.builder().build();
        executor.executeBackup( context );

        return backupDir.resolve( backupName );
    }
}
