/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.tx.FileCopyMonitor;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.monitoring.Monitors;

public class StreamToDiskProvider implements StoreFileStreamProvider
{
    private final File storeDir;
    private final FileSystemAbstraction fs;
    private final FileCopyMonitor fileCopyMonitor;

    public StreamToDiskProvider( File storeDir, FileSystemAbstraction fs, Monitors monitors )
    {
        this.storeDir = storeDir;
        this.fs = fs;
        this.fileCopyMonitor = monitors.newMonitor( FileCopyMonitor.class );
    }

    @Override
    public StoreFileStream acquire( String destination, int requiredAlignment ) throws IOException
    {
        File fileName = new File( storeDir, destination );
        fs.mkdirs( fileName.getParentFile() );
        fileCopyMonitor.copyFile( fileName );
        return StreamToDisk.fromFile( fs, fileName );
    }
}
