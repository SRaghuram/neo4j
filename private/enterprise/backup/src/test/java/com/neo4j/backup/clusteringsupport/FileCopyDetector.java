/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.clusteringsupport;

import com.neo4j.causalclustering.catchup.tx.FileCopyMonitor;

import java.io.File;

public class FileCopyDetector implements FileCopyMonitor
{
    private volatile boolean hasCopiedFile;

    @Override
    public void copyFile( File file )
    {
        hasCopiedFile = true;
    }

    boolean hasDetectedAnyFileCopied()
    {
        return hasCopiedFile;
    }
}
