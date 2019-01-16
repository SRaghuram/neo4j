/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.clusteringsupport.backup_stores;

import com.neo4j.causalclustering.common.Cluster;
import com.neo4j.causalclustering.core.CoreClusterMember;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFiles;

import static org.neo4j.backup.clusteringsupport.BackupUtil.createBackupFromCore;

public abstract class AbstractStoreGenerator implements BackupStore
{
    abstract CoreClusterMember createData( Cluster<?> cluster ) throws Exception;

    abstract void modify( File backup ) throws Exception;

    @Override
    public Optional<File> generate( File backupDir, Cluster<?> backupCluster ) throws Exception
    {
        CoreClusterMember core = createData( backupCluster );
        File backupFromCore = createBackupFromCore( core, backupName(), backupDir );
        modify( backupFromCore );
        return Optional.of( backupFromCore );
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

    private static String backupName()
    {
        return "backup-" + UUID.randomUUID().toString().substring( 5 );
    }
}
