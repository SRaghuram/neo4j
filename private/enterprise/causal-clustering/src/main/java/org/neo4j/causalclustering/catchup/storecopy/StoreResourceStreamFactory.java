/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.cursor.RawCursor;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.Database;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.neo4j.io.fs.FileUtils.relativePath;

public class StoreResourceStreamFactory
{
    private final FileSystemAbstraction fs;
    private final Supplier<Database> dataSourceSupplier;

    public StoreResourceStreamFactory( FileSystemAbstraction fs, Supplier<Database> dataSourceSupplier )
    {
        this.fs = fs;
        this.dataSourceSupplier = dataSourceSupplier;
    }

    RawCursor<StoreResource,IOException> create() throws IOException
    {
        Database dataSource = dataSourceSupplier.get();

        File databaseDirectory = dataSource.getDatabaseLayout().databaseDirectory();
        ResourceIterator<StoreFileMetadata> files = dataSource.listStoreFiles( false );

        return new RawCursor<StoreResource,IOException>()
        {
            private StoreResource resource;

            @Override
            public StoreResource get()
            {
                return resource;
            }

            @Override
            public boolean next() throws IOException
            {
                if ( !files.hasNext() )
                {
                    resource = null;
                    return false;
                }

                StoreFileMetadata md = files.next();

                resource = new StoreResource( md.file(), relativePath( databaseDirectory, md.file() ), md.recordSize(), fs );
                return true;
            }

            @Override
            public void close()
            {
                files.close();
            }
        };
    }
}
