/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;

public class TemporaryStoreDirectory implements AutoCloseable
{
    private static final String TEMP_COPY_DIRECTORY_NAME = "temp-copy";

    private final File tempStoreDir;
    private final DatabaseLayout tempDatabaseLayout;
    private final StoreFiles storeFiles;
    private LogFiles tempLogFiles;
    private boolean keepStore;

    public TemporaryStoreDirectory( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout databaseLayout ) throws IOException
    {

        this.tempStoreDir = databaseLayout.file( TEMP_COPY_DIRECTORY_NAME );
        this.tempDatabaseLayout = DatabaseLayout.of( tempStoreDir, databaseLayout.getDatabaseName() );
        storeFiles = new StoreFiles( fs, pageCache, ( directory, name ) -> true );
        tempLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( tempDatabaseLayout.databaseDirectory(), fs ).build();
        storeFiles.delete( tempStoreDir, tempLogFiles );
    }

    public File storeDir()
    {
        return tempStoreDir;
    }

    public DatabaseLayout databaseLayout()
    {
        return tempDatabaseLayout;
    }

    void keepStore()
    {
        this.keepStore = true;
    }

    @Override
    public void close() throws IOException
    {
        if ( !keepStore )
        {
            storeFiles.delete( tempStoreDir, tempLogFiles );
        }
    }
}
