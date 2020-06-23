/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.io.IOException;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.storageengine.api.StorageEngineFactory;

import static com.neo4j.configuration.CausalClusteringInternalSettings.TEMP_STORE_COPY_DIRECTORY_NAME;

public class TemporaryStoreDirectory implements AutoCloseable
{
    private final File tempHomeDir;
    private final DatabaseLayout tempDatabaseLayout;
    private final FileSystemAbstraction fs;
    private final StoreFiles storeFiles;
    private final LogFiles tempLogFiles;
    private boolean keepStore;

    TemporaryStoreDirectory( FileSystemAbstraction fs, PageCache pageCache, DatabaseLayout databaseLayout, StorageEngineFactory storageEngineFactory )
            throws IOException
    {
        this.tempHomeDir = databaseLayout.file( TEMP_STORE_COPY_DIRECTORY_NAME ).toFile();
        this.tempDatabaseLayout = Neo4jLayout.ofFlat( tempHomeDir.toPath() ).databaseLayout( databaseLayout.getDatabaseName() );
        this.fs = fs;
        storeFiles = new StoreFiles( fs, pageCache, ( directory, name ) -> true );
        tempLogFiles = LogFilesBuilder.logFilesBasedOnlyBuilder( tempDatabaseLayout.getTransactionLogsDirectory().toFile(), fs )
                .withCommandReaderFactory( storageEngineFactory.commandReaderFactory() )
                .build();
        storeFiles.delete( tempDatabaseLayout, tempLogFiles );
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
            storeFiles.delete( tempDatabaseLayout, tempLogFiles );
            fs.deleteFile( tempHomeDir );
        }
    }
}
