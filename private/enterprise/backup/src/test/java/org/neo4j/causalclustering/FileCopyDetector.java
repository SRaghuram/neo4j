/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering;

import java.io.File;

import org.neo4j.causalclustering.catchup.tx.FileCopyMonitor;

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
