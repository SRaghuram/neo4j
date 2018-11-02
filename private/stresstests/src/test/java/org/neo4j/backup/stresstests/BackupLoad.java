/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.backup.stresstests;

import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.backup.BackupHelper;
import org.neo4j.backup.BackupResult;
import org.neo4j.causalclustering.stresstests.Control;
import org.neo4j.helper.Workload;

class BackupLoad extends Workload
{

    private final String backupHostname;
    private final int backupPort;
    private final Path backupDir;

    BackupLoad( Control control, String backupHostname, int backupPort, Path backupDir )
    {
        super( control );
        this.backupHostname = backupHostname;
        this.backupPort = backupPort;
        this.backupDir = backupDir;
    }

    @Override
    protected void doWork() throws Exception
    {
        BackupResult backupResult = BackupHelper.backup( backupHostname, backupPort, backupDir );
        if ( !backupResult.isConsistent() )
        {
            throw new RuntimeException( "Inconsistent backup" );
        }
        if ( backupResult.isTransientErrorOnBackup() )
        {
            LockSupport.parkNanos( TimeUnit.MILLISECONDS.toNanos( 10 ) );
        }
    }
}
