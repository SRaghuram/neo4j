/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.clusteringsupport;

import com.neo4j.causalclustering.catchup.tx.FileCopyMonitor;

import java.io.File;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class FileCopyDetector implements FileCopyMonitor
{
    private Set<File> copiedFiles = ConcurrentHashMap.newKeySet();

    @Override
    public void copyFile( File file )
    {
        copiedFiles.add( file );
    }

    public boolean anyFileInDirectoryWithName( String directory )
    {
        return copiedFiles.stream().anyMatch( file -> file.getParentFile().getName().equals( directory ) );
    }
}
