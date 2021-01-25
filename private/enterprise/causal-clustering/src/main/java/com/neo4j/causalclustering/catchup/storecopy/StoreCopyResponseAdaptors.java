/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupErrorResponse;
import com.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import com.neo4j.causalclustering.catchup.CatchupResult;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import org.neo4j.logging.Log;

import static com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.LAST_CHECKPOINTED_TX_UNAVAILABLE;
import static java.lang.String.format;

public abstract class StoreCopyResponseAdaptors<T> extends CatchupResponseAdaptor<T>
{
    public static StoreCopyResponseAdaptors<StoreCopyFinishedResponse> filesCopyAdaptor( StoreFileStreamProvider storeFileStreamProvider, Log log )
    {
        return new StoreFilesCopyResponseAdaptors( storeFileStreamProvider, log );
    }

    public static StoreCopyResponseAdaptors<PrepareStoreCopyResponse> prepareStoreCopyAdaptor( StoreFileStreamProvider storeFileStreamProvider, Log log )
    {
        return new PrepareStoreCopyResponseAdaptors( storeFileStreamProvider, log );
    }

    private final StoreFileStreamProvider storeFileStreamProvider;
    private final Log log;
    private StoreFileStream storeFileStream;

    private StoreCopyResponseAdaptors( StoreFileStreamProvider storeFileStreamProvider, Log log )
    {
        this.storeFileStreamProvider = storeFileStreamProvider;
        this.log = log;
    }

    /**
     * Files will be sent in order but multiple files may be sent during one response.
     *
     * @param requestOutcomeSignal signal
     * @param fileHeader header for most resent file being sent
     */
    @Override
    public void onFileHeader( CompletableFuture<T> requestOutcomeSignal, FileHeader fileHeader )
    {
        try
        {
            final StoreFileStream fileStream = storeFileStreamProvider.acquire( fileHeader.fileName(), fileHeader.requiredAlignment() );
            // Make sure that each stream closes on complete but only the latest is written to
            requestOutcomeSignal.whenComplete( new CloseFileStreamOnComplete<>( fileStream, fileHeader.fileName() ) );
            this.storeFileStream = fileStream;
        }
        catch ( Exception e )
        {
            requestOutcomeSignal.completeExceptionally( e );
        }
    }

    @Override
    public boolean onFileContent( CompletableFuture<T> signal, FileChunk fileChunk )
    {
        try
        {
            storeFileStream.write( fileChunk.payload() );
        }
        catch ( Exception e )
        {
            signal.completeExceptionally( e );
        }
        return fileChunk.isLast();
    }

    private static class PrepareStoreCopyResponseAdaptors extends StoreCopyResponseAdaptors<PrepareStoreCopyResponse>
    {
        PrepareStoreCopyResponseAdaptors( StoreFileStreamProvider storeFileStreamProvider, Log log )
        {
            super( storeFileStreamProvider, log );
        }

        @Override
        public void onStoreListingResponse( CompletableFuture<PrepareStoreCopyResponse> signal, PrepareStoreCopyResponse response )
        {
            signal.complete( response );
        }
    }

    private static class StoreFilesCopyResponseAdaptors extends StoreCopyResponseAdaptors<StoreCopyFinishedResponse>
    {
        StoreFilesCopyResponseAdaptors( StoreFileStreamProvider storeFileStreamProvider, Log log )
        {
            super( storeFileStreamProvider, log );
        }

        @Override
        public void onFileStreamingComplete( CompletableFuture<StoreCopyFinishedResponse> signal, StoreCopyFinishedResponse response )
        {
            signal.complete( response );
        }

        /**
         * Method overridden to avoid failing the outcome future and throwing exceptions.
         * This is needed because all file copy requests can be retried.
         * Throwing exceptions in this case is just an overhead.
         * <p>
         * See retry logic in {@link StoreCopyClient} for more details.
         *
         * @param signal the outcome future.
         * @param catchupErrorResponse the response.
         */
        @Override
        public void onCatchupErrorResponse( CompletableFuture<StoreCopyFinishedResponse> signal, CatchupErrorResponse catchupErrorResponse )
        {
            StoreCopyFinishedResponse.Status status =
                    catchupErrorResponse.status() == CatchupResult.E_DATABASE_UNKNOWN ? StoreCopyFinishedResponse.Status.E_DATABASE_UNKNOWN
                                                                                      : StoreCopyFinishedResponse.Status.E_UNKNOWN;
            signal.complete( new StoreCopyFinishedResponse( status, LAST_CHECKPOINTED_TX_UNAVAILABLE ) );
        }
    }

    private class CloseFileStreamOnComplete<RESPONSE> implements BiConsumer<RESPONSE,Throwable>
    {
        private final StoreFileStream fileStream;
        private String fileName;

        private CloseFileStreamOnComplete( StoreFileStream fileStream, String fileName )
        {
            this.fileStream = fileStream;
            this.fileName = fileName;
        }

        @Override
        public void accept( RESPONSE response, Throwable throwable )
        {
            try
            {
                fileStream.close();
            }
            catch ( Exception e )
            {
                log.error( format( "Unable to close store file stream for file '%s'", fileName ), e );
            }
        }
    }
}
