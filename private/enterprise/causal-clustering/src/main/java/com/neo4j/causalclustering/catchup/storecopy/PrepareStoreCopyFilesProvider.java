/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.Database;

public class PrepareStoreCopyFilesProvider
{
    private final FileSystemAbstraction fileSystemAbstraction;

    public PrepareStoreCopyFilesProvider( FileSystemAbstraction fileSystemAbstraction )
    {
        this.fileSystemAbstraction = fileSystemAbstraction;
    }

    PrepareStoreCopyFiles prepareStoreCopyFiles( Database database )
    {
        return new PrepareStoreCopyFiles( database, fileSystemAbstraction );
    }
}
