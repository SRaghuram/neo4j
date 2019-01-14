/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com.storecopy;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.com.RequestContext;
import org.neo4j.com.Response;
import org.neo4j.com.ServerFailureException;
import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.OpenMode;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.checkpoint.StoreCopyCheckPointMutex;
import org.neo4j.storageengine.api.StoreFileMetadata;

import static org.neo4j.com.RequestContext.anonymous;
import static org.neo4j.io.fs.FileUtils.getCanonicalFile;
import static org.neo4j.io.fs.FileUtils.relativePath;

/**
 * Is able to feed store files in a consistent way to a {@link Response} to be picked up by a
 * {@link StoreCopyClient}, for example.
 *
 * @see StoreCopyClient
 */
public class StoreCopyServer
{
    public interface Monitor
    {
        void startTryCheckPoint( String storeCopyIdentifier );

        void finishTryCheckPoint( String storeCopyIdentifier );

        void startStreamingStoreFile( File file, String storeCopyIdentifier );

        void finishStreamingStoreFile( File file, String storeCopyIdentifier );

        void startStreamingStoreFiles( String storeCopyIdentifier );

        void finishStreamingStoreFiles( String storeCopyIdentifier );

        void startStreamingTransactions( long startTxId, String storeCopyIdentifier );

        void finishStreamingTransactions( long endTxId, String storeCopyIdentifier );

        class Adapter implements Monitor
        {
            @Override
            public void startTryCheckPoint( String storeCopyIdentifier )
            {   // empty
            }

            @Override
            public void finishTryCheckPoint( String storeCopyIdentifier )
            {   // empty
            }

            @Override
            public void startStreamingStoreFile( File file, String storeCopyIdentifier )
            {   // empty
            }

            @Override
            public void finishStreamingStoreFile( File file, String storeCopyIdentifier )
            {   // empty
            }

            @Override
            public void startStreamingStoreFiles( String storeCopyIdentifier )
            {   // empty
            }

            @Override
            public void finishStreamingStoreFiles( String storeCopyIdentifier )
            {   // empty
            }

            @Override
            public void startStreamingTransactions( long startTxId, String storeCopyIdentifier )
            {   // empty
            }

            @Override
            public void finishStreamingTransactions( long endTxId, String storeCopyIdentifier )
            {   // empty
            }
        }
    }

    private final NeoStoreDataSource dataSource;
    private final CheckPointer checkPointer;
    private final FileSystemAbstraction fileSystem;
    private final File databaseDirectory;
    private final Monitor monitor;

    public StoreCopyServer( NeoStoreDataSource dataSource, CheckPointer checkPointer, FileSystemAbstraction fileSystem,
            File databaseDirectory, Monitor monitor )
    {
        this.dataSource = dataSource;
        this.checkPointer = checkPointer;
        this.fileSystem = fileSystem;
        this.databaseDirectory = getCanonicalFile( databaseDirectory );
        this.monitor = monitor;
    }

    public Monitor monitor()
    {
        return monitor;
    }

    /**
     * Trigger store flush (checkpoint) and write {@link NeoStoreDataSource#listStoreFiles(boolean) store files} to the
     * given {@link StoreWriter}.
     *
     * @param triggerName name of the component asks for store files.
     * @param writer store writer to write files to.
     * @param includeLogs <code>true</code> if transaction logs should be copied, <code>false</code> otherwise.
     * @return a {@link RequestContext} specifying at which point the store copy started.
     */
    public RequestContext flushStoresAndStreamStoreFiles( String triggerName, StoreWriter writer, boolean includeLogs )
    {
        try
        {
            String storeCopyIdentifier = Thread.currentThread().getName();
            ThrowingAction<IOException> checkPointAction = () ->
            {
                monitor.startTryCheckPoint( storeCopyIdentifier );
                checkPointer.tryCheckPoint( new SimpleTriggerInfo( triggerName ) );
                monitor.finishTryCheckPoint( storeCopyIdentifier );
            };

            // Copy the store files
            long lastAppliedTransaction;
            StoreCopyCheckPointMutex mutex = dataSource.getStoreCopyCheckPointMutex();
            try ( Resource lock = mutex.storeCopy( checkPointAction );
                  ResourceIterator<StoreFileMetadata> files = dataSource.listStoreFiles( includeLogs ) )
            {
                lastAppliedTransaction = checkPointer.lastCheckPointedTransactionId();
                monitor.startStreamingStoreFiles( storeCopyIdentifier );
                ByteBuffer temporaryBuffer = ByteBuffer.allocateDirect( (int) ByteUnit.mebiBytes( 1 ) );
                while ( files.hasNext() )
                {
                    StoreFileMetadata meta = files.next();
                    File file = meta.file();
                    boolean isLogFile = meta.isLogFile();
                    int recordSize = meta.recordSize();

                    try ( ReadableByteChannel fileChannel = fileSystem.open( file, OpenMode.READ ) )
                    {
                        long fileSize = fileSystem.getFileSize( file );
                        doWrite( writer, temporaryBuffer, file, recordSize, fileChannel, fileSize,
                                storeCopyIdentifier, isLogFile );
                    }
                }
            }
            finally
            {
                monitor.finishStreamingStoreFiles( storeCopyIdentifier );
            }

            return anonymous( lastAppliedTransaction );
        }
        catch ( IOException e )
        {
            throw new ServerFailureException( e );
        }
    }

    private void doWrite( StoreWriter writer, ByteBuffer temporaryBuffer, File file, int recordSize,
            ReadableByteChannel fileChannel, long fileSize, String storeCopyIdentifier, boolean isLogFile ) throws IOException
    {
        monitor.startStreamingStoreFile( file, storeCopyIdentifier );
        String path = isLogFile ? file.getName() : relativePath( databaseDirectory, file );
        writer.write( path, fileChannel, temporaryBuffer, fileSize > 0, recordSize );
        monitor.finishStreamingStoreFile( file, storeCopyIdentifier );
    }
}
