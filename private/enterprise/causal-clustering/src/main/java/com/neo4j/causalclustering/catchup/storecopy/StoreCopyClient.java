/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import com.neo4j.causalclustering.catchup.VersionedCatchupClients;
import com.neo4j.causalclustering.catchup.VersionedCatchupClients.PreparedRequest;

import java.io.File;
import java.net.ConnectException;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

import static com.neo4j.causalclustering.catchup.storecopy.RequiredTransactions.noConstraint;
import static com.neo4j.causalclustering.catchup.storecopy.RequiredTransactions.requiredRange;
import static com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.LAST_CHECKPOINTED_TX_UNAVAILABLE;
import static java.lang.Long.max;
import static java.lang.String.format;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

public class StoreCopyClient
{
    private final CatchupClientFactory catchUpClientFactory;
    private final Supplier<Monitors> monitors;
    private final NamedDatabaseId namedDatabaseId;
    private final Log log;
    private final TimeoutStrategy backOffStrategy;

    public StoreCopyClient( CatchupClientFactory catchUpClientFactory, NamedDatabaseId namedDatabaseId, Supplier<Monitors> monitors, LogProvider logProvider,
            TimeoutStrategy backOffStrategy )
    {
        this.catchUpClientFactory = catchUpClientFactory;
        this.monitors = monitors;
        this.namedDatabaseId = namedDatabaseId;
        this.backOffStrategy = backOffStrategy;
        this.log = logProvider.getLog( getClass() );
    }

    public RequiredTransactions copyStoreFiles( CatchupAddressProvider catchupAddressProvider, StoreId expectedStoreId,
            StoreFileStreamProvider storeFileStreamProvider, Supplier<TerminationCondition> requestWiseTerminationCondition, File destDir )
            throws StoreCopyFailedException
    {
        try
        {
            SocketAddress fromAddress = catchupAddressProvider.primary( namedDatabaseId );
            PrepareStoreCopyResponse prepareStoreCopyResponse = prepareStoreCopy( fromAddress, expectedStoreId, storeFileStreamProvider );
            TransactionIdHandler txIdHandler = new TransactionIdHandler( prepareStoreCopyResponse );
            copyFilesIndividually( prepareStoreCopyResponse, expectedStoreId, catchupAddressProvider, storeFileStreamProvider, requestWiseTerminationCondition,
                    destDir, txIdHandler );
            return txIdHandler.requiredTransactionRange();
        }
        catch ( StoreCopyFailedException e )
        {
            throw e;
        }
        catch ( Exception e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    private void copyFilesIndividually( PrepareStoreCopyResponse prepareStoreCopyResponse, StoreId expectedStoreId, CatchupAddressProvider addressProvider,
            StoreFileStreamProvider storeFileStream, Supplier<TerminationCondition> terminationConditions, File destDir, TransactionIdHandler txIdHandler )
            throws StoreCopyFailedException
    {
        StoreCopyClientMonitor
                storeCopyClientMonitor = monitors.get().newMonitor( StoreCopyClientMonitor.class );
        storeCopyClientMonitor.startReceivingStoreFiles();
        long lastCheckPointedTxId = prepareStoreCopyResponse.lastCheckPointedTransactionId();

        for ( File file : prepareStoreCopyResponse.getFiles() )
        {
            storeCopyClientMonitor.startReceivingStoreFile( Paths.get( destDir.toString(), file.getName() ).toString() );

            persistentCallToSecondary( addressProvider,
                    c -> c.getStoreFile( expectedStoreId, file, lastCheckPointedTxId, namedDatabaseId ),
                    storeFileStream, terminationConditions.get(), txIdHandler );

            storeCopyClientMonitor.finishReceivingStoreFile( Paths.get( destDir.toString(), file.getName() ).toString() );
        }
        storeCopyClientMonitor.finishReceivingStoreFiles();
    }

    private void persistentCallToSecondary( CatchupAddressProvider addressProvider,
            Function<VersionedCatchupClients.CatchupClientV3,PreparedRequest<StoreCopyFinishedResponse>> v3Request,
            StoreFileStreamProvider storeFileStream,
            TerminationCondition terminationCondition, TransactionIdHandler txIdHandler ) throws StoreCopyFailedException
    {
        var successful = false;
        TimeoutStrategy.Timeout timeout = backOffStrategy.newTimeout();

        while ( true )
        {
            Collection<SocketAddress> secondaries = List.of();

            try
            {
                secondaries = addressProvider.allSecondaries( namedDatabaseId );
            }
            catch ( CatchupAddressResolutionException e )
            {
                log.warn( "Unable to resolve address for StoreCopyRequest. %s", e.getMessage() );
            }

            for ( var secondary : secondaries )
            {
                successful = requestToSecondary( secondary, storeFileStream, txIdHandler, v3Request );
                if ( successful )
                {
                    return;
                }
            }
            awaitAndIncrementTimeout( timeout );
            terminationCondition.assertContinue();
        }
    }

    private boolean requestToSecondary( SocketAddress secondary, StoreFileStreamProvider storeFileStream, TransactionIdHandler txIdHandler,
            Function<VersionedCatchupClients.CatchupClientV3,PreparedRequest<StoreCopyFinishedResponse>> v3Request )
    {
        var successfulRequest = false;
        try
        {
            log.info(format("Sending request StoreCopyRequest to '%s'", secondary));

            StoreCopyFinishedResponse response = catchUpClientFactory.getClient(secondary, log)
                    .v3(v3Request)
                    .withResponseHandler(StoreCopyResponseAdaptors.filesCopyAdaptor(storeFileStream, log))
                    .request();

            successfulRequest = successfulRequest( response );

            if ( successfulRequest )
            {
                txIdHandler.handle( response );
            }
        }
        catch ( ConnectException e )
        {
            log.warn( "Unable to connect. %s", e.getMessage() );
        }
        catch ( Exception e )
        {
            //TODO: I understood the argument that we should just throw and catch Exception because anything can go wrong with a future/network request, but
            // it seems like we're at risk of swallowing runtime exceptions in some cases where we otherwise wouldn't
            log.warn( "StoreCopyRequest failed exceptionally.", e );
        }

        return successfulRequest;
    }

    private static void awaitAndIncrementTimeout( TimeoutStrategy.Timeout timeout ) throws StoreCopyFailedException
    {
        try
        {
            Thread.sleep( timeout.getMillis() );
            timeout.increment();
        }
        catch ( InterruptedException e )
        {
            throw new StoreCopyFailedException( "Thread interrupted" );
        }
    }

    private PrepareStoreCopyResponse prepareStoreCopy( SocketAddress from, StoreId expectedStoreId, StoreFileStreamProvider storeFileStream )
            throws StoreCopyFailedException
    {
        PrepareStoreCopyResponse prepareStoreCopyResponse;
        try
        {
            log.info( "Requesting store listing from: " + from );
            prepareStoreCopyResponse = catchUpClientFactory.getClient( from, log )
                    .v3( c -> c.prepareStoreCopy( expectedStoreId, namedDatabaseId ) )
                    .withResponseHandler( StoreCopyResponseAdaptors.prepareStoreCopyAdaptor( storeFileStream, log ) )
                    .request();
        }
        catch ( Exception e )
        {
            throw new StoreCopyFailedException( e );
        }

        if ( prepareStoreCopyResponse.status() != PrepareStoreCopyResponse.Status.SUCCESS )
        {
            throw new StoreCopyFailedException( "Preparing store failed due to: " + prepareStoreCopyResponse.status() );
        }
        return prepareStoreCopyResponse;
    }

    public StoreId fetchStoreId( SocketAddress fromAddress ) throws StoreIdDownloadFailedException
    {
        try
        {
            CatchupResponseAdaptor<StoreId> responseHandler = new CatchupResponseAdaptor<>()
            {
                @Override
                public void onGetStoreIdResponse( CompletableFuture<StoreId> signal, GetStoreIdResponse response )
                {
                    signal.complete( response.storeId() );
                }
            };
            return catchUpClientFactory.getClient( fromAddress, log )
                    .v3( c -> c.getStoreId( namedDatabaseId ) )
                    .withResponseHandler( responseHandler )
                    .request();
        }
        catch ( Exception e )
        {
            throw new StoreIdDownloadFailedException( e );
        }
    }

    private boolean successfulRequest( StoreCopyFinishedResponse response ) throws StoreCopyFailedException
    {
        switch ( response.status() )
        {
            case SUCCESS:
                log.info( "StoreCopyRequest was successful." );
                return true;
            case E_TOO_FAR_BEHIND:
            case E_UNKNOWN:
            case E_STORE_ID_MISMATCH:
            case E_DATABASE_UNKNOWN:
                log.warn( format( "StoreCopyRequest failed with response: %s", response.status() ) );
                return false;
            default:
                throw new StoreCopyFailedException( format( "Request responded with an unknown response type: %s.", response.status() ) );
        }
    }

    private static class TransactionIdHandler
    {
        /**
         * Represents the minimal transaction ID that can be committed by the user.
         */
        private static final long MIN_COMMITTED_TRANSACTION_ID = BASE_TX_ID + 1;
        private final long initialTxId;
        private long highestReceivedTxId = LAST_CHECKPOINTED_TX_UNAVAILABLE;

        TransactionIdHandler( PrepareStoreCopyResponse prepareStoreCopyResponse )
        {
            this.initialTxId = prepareStoreCopyResponse.lastCheckPointedTransactionId();
        }

        void handle( StoreCopyFinishedResponse response )
        {
            if ( response.status() == StoreCopyFinishedResponse.Status.SUCCESS )
            {
                highestReceivedTxId = max( highestReceivedTxId, response.lastCheckpointedTx() );
            }
        }

        RequiredTransactions requiredTransactionRange()
        {
            return highestReceivedTxId < MIN_COMMITTED_TRANSACTION_ID ? noConstraint( max( initialTxId, MIN_COMMITTED_TRANSACTION_ID ) )
                                                                      : requiredRange( initialTxId, highestReceivedTxId );
        }
    }
}
