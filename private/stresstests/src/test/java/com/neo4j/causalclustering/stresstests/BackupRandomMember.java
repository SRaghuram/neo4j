/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import com.neo4j.causalclustering.common.ClusterMember;

import java.nio.file.Path;
import java.util.Optional;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.logging.Log;

class BackupRandomMember extends RepeatOnRandomMember
{
    private final Log log;
    private final BackupHelper backupHelper;
    private final FileSystemAbstraction fs;

    BackupRandomMember( Control control, Resources resources )
    {
        super( control, resources );
        this.log = resources.logProvider().getLog( getClass() );
        this.fs = resources.fileSystem();
        this.backupHelper = new BackupHelper( resources );
    }

    @Override
    public void doWorkOnMember( ClusterMember member ) throws Exception
    {
        Optional<Path> backupDir = backupHelper.backup( member );
        if ( backupDir.isPresent() )
        {
            fs.deleteRecursively( backupDir.get() );
        }
    }

    @Override
    public void validate()
    {
        if ( backupHelper.successfulBackups.get() == 0 )
        {
            throw new IllegalStateException( "Failed to perform any backups" );
        }

        log.info( String.format( "Performed %d/%d successful backups.", backupHelper.successfulBackups.get(), backupHelper.backupNumber.get() ) );
    }
}
