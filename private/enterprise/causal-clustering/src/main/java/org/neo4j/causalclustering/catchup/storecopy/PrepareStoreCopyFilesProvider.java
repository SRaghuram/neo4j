/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.NeoStoreDataSource;

public class PrepareStoreCopyFilesProvider
{
    private final FileSystemAbstraction fileSystemAbstraction;

    public PrepareStoreCopyFilesProvider( FileSystemAbstraction fileSystemAbstraction )
    {
        this.fileSystemAbstraction = fileSystemAbstraction;
    }

    PrepareStoreCopyFiles prepareStoreCopyFiles( NeoStoreDataSource neoStoreDataSource )
    {
        return new PrepareStoreCopyFiles( neoStoreDataSource, fileSystemAbstraction );
    }
}
