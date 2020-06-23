/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.io.IOException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.Database;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.neo4j.io.fs.FileUtils.relativePath;

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
        ResourceIterator<StoreFileMetadata> neoStoreFilesIterator =
                closeablesListener.add( database.getDatabaseFileListing().builder().excludeAll().includeNeoStoreFiles().build() );
        ResourceIterator<StoreFileMetadata> indexIterator = closeablesListener.add( database
                .getDatabaseFileListing()
                .builder()
                .excludeAll()
                .includeAdditionalProviders()
                .includeLabelScanStoreFiles()
                .includeRelationshipTypeScanStoreFiles()
                .includeSchemaIndexStoreFiles()
                .includeIdFiles()
                .build() );

        return Stream.concat( neoStoreFilesIterator.stream().filter( isCountFile( database.getDatabaseLayout() ) ), indexIterator.stream() ).map(
                mapToStoreResource() ).toArray( StoreResource[]::new );
    }

    private Function<StoreFileMetadata,StoreResource> mapToStoreResource()
    {
        return storeFileMetadata ->
        {
            try
            {
                return toStoreResource( storeFileMetadata );
            }
            catch ( IOException e )
            {
                throw new IllegalStateException( "Unable to create store resource", e );
            }
        };
    }

    File[] listReplayableFiles() throws IOException
    {
        try ( Stream<StoreFileMetadata> stream = database.getDatabaseFileListing().builder()
                .excludeAll()
                .includeNeoStoreFiles()
                .build().stream() )
        {
            return stream.filter( isCountFile( database.getDatabaseLayout() ).negate() ).map( StoreFileMetadata::file ).toArray( File[]::new );
        }
    }

    private static Predicate<StoreFileMetadata> isCountFile( DatabaseLayout databaseLayout )
    {
        return storeFileMetadata -> databaseLayout.countStore().toFile().equals( storeFileMetadata.file() );
    }

    private StoreResource toStoreResource( StoreFileMetadata storeFileMetadata ) throws IOException
    {
        File databaseDirectory = database.getDatabaseLayout().databaseDirectory().toFile();
        File file = storeFileMetadata.file();
        String relativePath = relativePath( databaseDirectory, file );
        return new StoreResource( file, relativePath, storeFileMetadata.recordSize(), fileSystemAbstraction );
    }

    @Override
    public void close()
    {
        closeablesListener.close();
    }
}
