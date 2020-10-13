/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.io.IOException;
import java.nio.file.Path;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.Database;
import org.neo4j.storageengine.api.StoreFileMetadata;

public class PrepareStoreCopyFiles implements AutoCloseable
{
    private final Database database;
    private final FileSystemAbstraction fileSystemAbstraction;
    private final CloseablesListener closeablesListener = new CloseablesListener();

    PrepareStoreCopyFiles( Database database, FileSystemAbstraction fileSystemAbstraction )
    {
        this.database = database;
        this.fileSystemAbstraction = fileSystemAbstraction;
    }

    StoreResource[] getAtomicFilesSnapshot() throws IOException
    {
        ResourceIterator<StoreFileMetadata> resources = closeablesListener.add( database.getDatabaseFileListing().builder().excludeAll()
                .includeAtomicStorageFiles()
                .includeAdditionalProviders()
                .includeLabelScanStoreFiles()
                .includeRelationshipTypeScanStoreFiles()
                .includeSchemaIndexStoreFiles()
                .includeIdFiles()
                .build() );
        return resources.stream().map( this::toStoreResource ).toArray( StoreResource[]::new );
    }

    Path[] listReplayableFiles() throws IOException
    {
        return database.getDatabaseFileListing().builder().excludeAll()
                .includeReplayableStorageFiles()
                .build().stream().map( StoreFileMetadata::path ).toArray( Path[]::new );
    }

    private StoreResource toStoreResource( StoreFileMetadata storeFileMetadata )
    {
        Path databaseDirectory = database.getDatabaseLayout().databaseDirectory();
        Path file = storeFileMetadata.path();
        String relativePath = databaseDirectory.relativize( file ).toString();
        return new StoreResource( file, relativePath, storeFileMetadata.recordSize(), fileSystemAbstraction );
    }

    @Override
    public void close()
    {
        closeablesListener.close();
    }
}
