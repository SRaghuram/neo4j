/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.tx.FileCopyMonitor;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.monitoring.Monitors;

public class StreamToDiskProvider implements StoreFileStreamProvider
{
    private final Path storeDir;
    private final FileSystemAbstraction fs;
    private final FileCopyMonitor fileCopyMonitor;

    public StreamToDiskProvider( Path storeDir, FileSystemAbstraction fs, Monitors monitors )
    {
        this.storeDir = storeDir;
        this.fs = fs;
        this.fileCopyMonitor = monitors.newMonitor( FileCopyMonitor.class );
    }

    @Override
    public StoreFileStream acquire( String destination, int requiredAlignment ) throws IOException
    {
        Path fileName = storeDir.resolve( destination );
        fs.mkdirs( fileName.getParent() );
        fileCopyMonitor.copyFile( fileName );
        return StreamToDisk.fromFile( fs, fileName );
    }
}
