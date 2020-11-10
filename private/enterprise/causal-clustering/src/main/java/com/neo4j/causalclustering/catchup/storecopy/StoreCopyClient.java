/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupAddressProvider;
import com.neo4j.causalclustering.catchup.CatchupClientFactory;
import com.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import com.neo4j.causalclustering.catchup.VersionedCatchupClients.PreparedRequest;

import java.net.ConnectException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.TimeoutStrategy;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.StoreId;

import static com.neo4j.causalclustering.catchup.VersionedCatchupClients.CatchupClientV3;
import static com.neo4j.causalclustering.catchup.VersionedCatchupClients.CatchupClientV4;
import static com.neo4j.causalclustering.catchup.VersionedCatchupClients.CatchupClientV5;
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
    private final Executor executor;
    private final TimeoutStrategy backOffStrategy;
    private final Clock clock;

    public StoreCopyClient( CatchupClientFactory catchUpClientFactory, NamedDatabaseId namedDatabaseId, Supplier<Monitors> monitors, LogProvider logProvider,
                            Executor executor, TimeoutStrategy backOffStrategy, Clock clock )
    {
        this.catchUpClientFactory = catchUpClientFactory;
        this.monitors = monitors;
        this.namedDatabaseId = namedDatabaseId;
        this.log = logProvider.getLog( getClass() );
        this.executor = executor;
        this.clock = clock;
        this.backOffStrategy = backOffStrategy;
    }

    public RequiredTransactions copyStoreFiles( CatchupAddressProvider catchupAddressProvider, StoreId expectedStoreId,
                                                StoreFileStreamProvider storeFileStreamProvider, Supplier<TerminationCondition> requestWiseTerminationCondition,
                                                Path destDir )
            throws StoreCopyFailedException
    {
        try
        {
            SocketAddress fromAddress = catchupAddressProvider.primary( namedDatabaseId );
            PrepareStoreCopyResponse prepareStoreCopyResponse = prepareStoreCopy( fromAddress, expectedStoreId, storeFileStreamProvider );
            TransactionIdHandler txIdHandler = new TransactionIdHandler( prepareStoreCopyResponse );
            copyFiles( prepareStoreCopyResponse, expectedStoreId, catchupAddressProvider, storeFileStreamProvider, requestWiseTerminationCondition,
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

    private void copyFiles( PrepareStoreCopyResponse prepareStoreCopyResponse, StoreId expectedStoreId, CatchupAddressProvider addressProvider,
                            StoreFileStreamProvider storeFileStream, Supplier<TerminationCondition> terminationConditions, Path destDir,
                            TransactionIdHandler txIdHandler ) throws StoreCopyFailedException
    {
        final var inputPaths = Arrays.stream( prepareStoreCopyResponse.getPaths() ).collect( Collectors.toList() );
        final var repository = new AddressRepository( addressProvider, namedDatabaseId, clock, backOffStrategy, log );
        var storeCopyClientMonitor = monitors.get().newMonitor( StoreCopyClientMonitor.class );
        storeCopyClientMonitor.startReceivingStoreFiles();

        long lastCheckPointedTxId = prepareStoreCopyResponse.lastCheckPointedTransactionId();

        try
        {
            executeCopyFiles( expectedStoreId, storeFileStream, destDir, txIdHandler, inputPaths, repository, storeCopyClientMonitor, lastCheckPointedTxId,
                              terminationConditions );
        }
        finally
        {
            storeCopyClientMonitor.finishReceivingStoreFiles();
        }
    }

    void executeCopyFiles( StoreId expectedStoreId, StoreFileStreamProvider storeFileStream, Path destDir, TransactionIdHandler txIdHandler,
                           List<Path> filesToDownload, AddressRepository repository, StoreCopyClientMonitor storeCopyClientMonitor,
                           long lastCheckPointedTxId, Supplier<TerminationCondition> terminationConditions )
            throws StoreCopyFailedException
    {
        final var pathsToBeProcessed = new ArrayBlockingQueue<>( filesToDownload.size(), false, filesToDownload );
        CopyOnWriteArrayList<StoreCopyFailedException> storeCopyExceptions = new CopyOnWriteArrayList<>();
        final var copiedFiles = new CopyOnWriteArrayList<>();
        while ( copiedFiles.size() < filesToDownload.size() )
        {
            if ( !storeCopyExceptions.isEmpty() )
            {
                throw storeCopyExceptions.get( 0 );
            }
            final var nextPath = pathsToBeProcessed.poll();
            if ( nextPath != null )
            {
                executor.execute( () ->
                                  {
                                      final var nextFreeAddress = repository.nextFreeAddress();
                                      if ( nextFreeAddress.isEmpty() )
                                      {
                                          pathsToBeProcessed.add( nextPath );
                                          return;
                                      }

                                      storeCopyClientMonitor.startReceivingStoreFile( destDir.resolve( nextPath.getFileName() ).toString() );
                                      var response = requestToSecondary( nextFreeAddress.get(), storeFileStream,
                                                                         getStoreFilesV3( expectedStoreId, lastCheckPointedTxId, nextPath ),
                                                                         getStoreFilesV4( expectedStoreId, lastCheckPointedTxId, nextPath ),
                                                                         getStoreFilesV5( expectedStoreId, lastCheckPointedTxId, nextPath ) );
                                      final var successfulResponse = response.map( v -> successfulRequest( v, storeCopyExceptions ) ).orElse( false );
                                      if ( successfulResponse )
                                      {
                                          storeCopyClientMonitor.finishReceivingStoreFile( destDir.resolve( nextPath.getFileName() ).toString() );
                                          copiedFiles.add( nextPath );
                                          txIdHandler.handle( response.get() );
                                          repository.release( nextFreeAddress.get() );
                                      }
                                      else
                                      {
                                          pathsToBeProcessed.add( nextPath );
                                          repository.releaseAndPenalise( nextFreeAddress.get() );
                                      }
                                  } );
            }
            else
            {
                sleep( 100 );
            }
            terminationConditions.get().assertContinue();
        }
    }

    private void sleep( long milliseconds )
    {
        try
        {
            Thread.sleep( milliseconds );
        }
        catch ( InterruptedException ignored )
        {
        }
    }

    private Optional<StoreCopyFinishedResponse> requestToSecondary( SocketAddress address, StoreFileStreamProvider storeFileStream,
                                                                    Function<CatchupClientV3,PreparedRequest<StoreCopyFinishedResponse>> v3Request,
                                                                    Function<CatchupClientV4,PreparedRequest<StoreCopyFinishedResponse>> v4Request,
                                                                    Function<CatchupClientV5,PreparedRequest<StoreCopyFinishedResponse>> v5Request )
    {
        try
        {

            log.info( "Sending request StoreCopyRequest to '%s'", address );

            final var response = catchUpClientFactory.getClient( address, log )
                                                     .v3( v3Request )
                                                     .v4( v4Request )
                                                     .v5( v5Request )
                                                     .withResponseHandler( StoreCopyResponseAdaptors.filesCopyAdaptor( storeFileStream, log ) )
                                                     .request();
            return Optional.of( response );
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
        return Optional.empty();
    }

    private PrepareStoreCopyResponse prepareStoreCopy( SocketAddress from, StoreId expectedStoreId, StoreFileStreamProvider storeFileStream )
            throws StoreCopyFailedException
    {
        PrepareStoreCopyResponse prepareStoreCopyResponse;
        try
        {
            log.info( "Requesting store listing from: %s", from );
            prepareStoreCopyResponse = catchUpClientFactory.getClient( from, log )
                                                           .v3( c -> c.prepareStoreCopy( expectedStoreId, namedDatabaseId ) )
                                                           .v4( c -> c.prepareStoreCopy( expectedStoreId, namedDatabaseId ) )
                                                           .v5( c -> c.prepareStoreCopy( expectedStoreId, namedDatabaseId ) )
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
                                       .v4( c -> c.getStoreId( namedDatabaseId ) )
                                       .v5( c -> c.getStoreId( namedDatabaseId ) )
                                       .withResponseHandler( responseHandler )
                                       .request();
        }
        catch ( Exception e )
        {
            throw new StoreIdDownloadFailedException( e );
        }
    }

    private boolean successfulRequest( StoreCopyFinishedResponse response, CopyOnWriteArrayList<StoreCopyFailedException> exceptions )
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
            log.warn( "StoreCopyRequest failed with response: %s.", response.status() );
            return false;
        default:
            exceptions.add( new StoreCopyFailedException( format( "Request responded with an unknown response type: %s.", response.status() ) ) );
            return false;
        }
    }

    private Function<CatchupClientV3,PreparedRequest<StoreCopyFinishedResponse>> getStoreFilesV3( StoreId expectedStoreId, long lastCheckPointedTxId,
                                                                                                  Path path )
    {
        return c -> c.getStoreFile( expectedStoreId, path, lastCheckPointedTxId, namedDatabaseId );
    }

    private Function<CatchupClientV4,PreparedRequest<StoreCopyFinishedResponse>> getStoreFilesV4( StoreId expectedStoreId, long lastCheckPointedTxId,
                                                                                                  Path path )
    {
        return c -> c.getStoreFile( expectedStoreId, path, lastCheckPointedTxId, namedDatabaseId );
    }

    private Function<CatchupClientV5,PreparedRequest<StoreCopyFinishedResponse>> getStoreFilesV5( StoreId expectedStoreId, long lastCheckPointedTxId,
                                                                                                  Path path )
    {
        return c -> c.getStoreFile( expectedStoreId, path, lastCheckPointedTxId, namedDatabaseId );
    }

    static class TransactionIdHandler
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

        synchronized void handle( StoreCopyFinishedResponse response )
        {
            if ( response.status() == StoreCopyFinishedResponse.Status.SUCCESS )
            {
                highestReceivedTxId = max( highestReceivedTxId, response.lastCheckpointedTx() );
            }
        }

        synchronized RequiredTransactions requiredTransactionRange()
        {
            return highestReceivedTxId < MIN_COMMITTED_TRANSACTION_ID ? noConstraint( max( initialTxId, MIN_COMMITTED_TRANSACTION_ID ) )
                                                                      : requiredRange( initialTxId, highestReceivedTxId );
        }
    }
}
