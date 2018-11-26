/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import org.eclipse.collections.api.iterator.LongIterator;

import java.io.File;
import java.net.ConnectException;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupClientFactory;
import org.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import org.neo4j.causalclustering.catchup.CatchupAddressProvider;
import org.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import org.neo4j.causalclustering.catchup.VersionedCatchupClients.CatchupClientV1;
import org.neo4j.causalclustering.catchup.VersionedCatchupClients.CatchupClientV2;
import org.neo4j.causalclustering.catchup.VersionedCatchupClients.PreparedRequest;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.com.storecopy.StoreCopyClientMonitor;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyResponseAdaptors.filesCopyAdaptor;
import static org.neo4j.causalclustering.catchup.storecopy.StoreCopyResponseAdaptors.prepareStoreCopyAdaptor;

public class StoreCopyClient
{
    private final CatchupClientFactory catchUpClientFactory;
    private final Supplier<Monitors> monitors;
    private final String databaseName;
    private final Log log;
    private final TimeoutStrategy backOffStrategy;

    public StoreCopyClient( CatchupClientFactory catchUpClientFactory, String databaseName, Supplier<Monitors> monitors, LogProvider logProvider,
            TimeoutStrategy backOffStrategy )
    {
        this.catchUpClientFactory = catchUpClientFactory;
        this.monitors = monitors;
        this.databaseName = databaseName;
        this.backOffStrategy = backOffStrategy;
        this.log = logProvider.getLog( getClass() );
    }

    long copyStoreFiles( CatchupAddressProvider catchupAddressProvider, StoreId expectedStoreId, StoreFileStreamProvider storeFileStreamProvider,
            Supplier<TerminationCondition> requestWiseTerminationCondition, File destDir )
            throws StoreCopyFailedException
    {
        try
        {
            PrepareStoreCopyResponse prepareStoreCopyResponse = prepareStoreCopy( catchupAddressProvider.primary(), expectedStoreId, storeFileStreamProvider );
            copyFilesIndividually( prepareStoreCopyResponse, expectedStoreId, catchupAddressProvider, storeFileStreamProvider,
                    requestWiseTerminationCondition, destDir );
            copyIndexSnapshotIndividually( prepareStoreCopyResponse, expectedStoreId, catchupAddressProvider, storeFileStreamProvider,
                    requestWiseTerminationCondition );
            return prepareStoreCopyResponse.lastTransactionId();
        }
        catch ( Exception e )
        {
            throw new StoreCopyFailedException( e );
        }
    }

    private void copyFilesIndividually( PrepareStoreCopyResponse prepareStoreCopyResponse, StoreId expectedStoreId, CatchupAddressProvider addressProvider,
            StoreFileStreamProvider storeFileStream, Supplier<TerminationCondition> terminationConditions, File destDir ) throws StoreCopyFailedException
    {
        StoreCopyClientMonitor
                storeCopyClientMonitor = monitors.get().newMonitor( StoreCopyClientMonitor.class );
        storeCopyClientMonitor.startReceivingStoreFiles();
        long lastTransactionId = prepareStoreCopyResponse.lastTransactionId();

        for ( File file : prepareStoreCopyResponse.getFiles() )
        {
            storeCopyClientMonitor.startReceivingStoreFile( Paths.get( destDir.toString(), file.getName() ).toString() );

            persistentCallToSecondary( addressProvider,
                    c -> c.getStoreFile( expectedStoreId, file, lastTransactionId ),
                    c -> c.getStoreFile( expectedStoreId, file, lastTransactionId, databaseName ),
                    storeFileStream,
                    terminationConditions.get() );

            storeCopyClientMonitor.finishReceivingStoreFile( Paths.get( destDir.toString(), file.getName() ).toString() );
        }
        storeCopyClientMonitor.finishReceivingStoreFiles();
    }

    private void copyIndexSnapshotIndividually( PrepareStoreCopyResponse prepareStoreCopyResponse, StoreId expectedStoreId,
            CatchupAddressProvider addressProvider, StoreFileStreamProvider storeFileStream, Supplier<TerminationCondition> terminationConditions )
            throws StoreCopyFailedException
    {
        StoreCopyClientMonitor
                storeCopyClientMonitor = monitors.get().newMonitor( StoreCopyClientMonitor.class );
        long lastTransactionId = prepareStoreCopyResponse.lastTransactionId();
        LongIterator indexIds = prepareStoreCopyResponse.getIndexIds().longIterator();
        storeCopyClientMonitor.startReceivingIndexSnapshots();

        while ( indexIds.hasNext() )
        {
            long indexId = indexIds.next();
            storeCopyClientMonitor.startReceivingIndexSnapshot( indexId );

            persistentCallToSecondary( addressProvider,
                    c -> c.getIndexFiles( expectedStoreId, indexId, lastTransactionId ),
                    c -> c.getIndexFiles( expectedStoreId, indexId, lastTransactionId, databaseName ),
                    storeFileStream,
                    terminationConditions.get() );

            storeCopyClientMonitor.finishReceivingIndexSnapshot( indexId );
        }
        storeCopyClientMonitor.finishReceivingIndexSnapshots();
    }

    private void persistentCallToSecondary( CatchupAddressProvider addressProvider,
            Function<CatchupClientV1,PreparedRequest<StoreCopyFinishedResponse>> v1Request,
            Function<CatchupClientV2,PreparedRequest<StoreCopyFinishedResponse>> v2Request,
            StoreFileStreamProvider storeFileStream,
            TerminationCondition terminationCondition ) throws StoreCopyFailedException
    {
        TimeoutStrategy.Timeout timeout = backOffStrategy.newTimeout();
        while ( true )
        {
            try
            {
                AdvertisedSocketAddress address = addressProvider.secondary();
                log.info( format( "Sending request StoreCopyRequest to '%s'", address ) );

                StoreCopyFinishedResponse response = catchUpClientFactory.getClient( address )
                        .v1( v1Request )
                        .v2( v2Request )
                        .withResponseHandler( filesCopyAdaptor( storeFileStream, log ) )
                        .request( log );

                if ( successfulRequest( response ) )
                {
                    break;
                }
            }
            catch ( CatchupAddressResolutionException e )
            {
                log.warn( "Unable to resolve address for StoreCopyRequest. %s", e.getMessage() );
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
            terminationCondition.assertContinue();
            awaitAndIncrementTimeout( timeout );
        }
    }

    private void awaitAndIncrementTimeout( TimeoutStrategy.Timeout timeout ) throws StoreCopyFailedException
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

    private PrepareStoreCopyResponse prepareStoreCopy( AdvertisedSocketAddress from, StoreId expectedStoreId, StoreFileStreamProvider storeFileStream )
            throws StoreCopyFailedException
    {
        PrepareStoreCopyResponse prepareStoreCopyResponse;
        try
        {
            log.info( "Requesting store listing from: " + from );
            prepareStoreCopyResponse = catchUpClientFactory.getClient( from )
                    .v1( c -> c.prepareStoreCopy( expectedStoreId ) )
                    .v2( c -> c.prepareStoreCopy( expectedStoreId, databaseName ) )
                    .withResponseHandler( prepareStoreCopyAdaptor( storeFileStream, log ) )
                    .request( log );
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

    public StoreId fetchStoreId( AdvertisedSocketAddress fromAddress ) throws StoreIdDownloadFailedException
    {
        try
        {
            CatchupResponseAdaptor<StoreId> responseHandler = new CatchupResponseAdaptor<StoreId>()
            {
                @Override
                public void onGetStoreIdResponse( CompletableFuture<StoreId> signal, GetStoreIdResponse response )
                {
                    signal.complete( response.storeId() );
                }
            };
            return catchUpClientFactory.getClient( fromAddress )
                    .v1( CatchupClientV1::getStoreId )
                    .v2( c -> c.getStoreId( databaseName ) )
                    .withResponseHandler( responseHandler )
                    .request( log );
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
}
