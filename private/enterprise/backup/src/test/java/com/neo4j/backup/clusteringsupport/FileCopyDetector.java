/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.clusteringsupport;

import com.neo4j.causalclustering.catchup.tx.FileCopyMonitor;

import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class FileCopyDetector implements FileCopyMonitor
{
    private final Set<Path> copiedFiles = ConcurrentHashMap.newKeySet();

    @Override
    public void copyFile( Path path )
    {
        copiedFiles.add( path );
    }

    public boolean anyFileInDirectoryWithName( String directory )
    {
        return copiedFiles.stream().anyMatch( file -> file.getParent().getFileName().toString().equals( directory ) );
    }
}
