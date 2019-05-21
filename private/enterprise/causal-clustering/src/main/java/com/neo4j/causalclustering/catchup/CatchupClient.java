/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreFileRequest;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreIdRequest;
import com.neo4j.causalclustering.catchup.v3.storecopy.PrepareStoreCopyRequest;
import com.neo4j.causalclustering.catchup.v3.tx.TxPullRequest;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequest;
import com.neo4j.causalclustering.helper.OperationProgressMonitor;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.storageengine.api.StoreId;

class CatchupClient implements VersionedCatchupClients
{
    private static final int REQUEST_SENT_TIMEOUT = 1;
    private static final TimeUnit REQUEST_SENT_TIME_UNIT = TimeUnit.MINUTES;
    private final CompletableFuture<CatchupChannel> channelFuture;
    private final Duration inactivityTimeout;
    private final Log log;

    CatchupClient( CompletableFuture<CatchupChannel> channelFuture, Duration inactivityTimeout, Log log )
    {
        this.channelFuture = channelFuture;
        this.inactivityTimeout = inactivityTimeout;
        this.log = log;
    }

    private static <RESULT> CompletableFuture<RESULT> makeBlockingRequest( CatchupProtocolMessage request, CatchupResponseCallback<RESULT> responseHandler,
            CatchupChannel channel )
    {
        CompletableFuture<RESULT> future = new CompletableFuture<>();
        try
        {
            future.whenComplete( new ReleaseOnComplete( channel ) );
            channel.setResponseHandler( responseHandler, future );
            channel.send( request );
        }
        catch ( Exception e )
        {
            future.completeExceptionally( new CatchUpClientException( "Failed to send request", e ) );
        }

        return future;
    }

    @Override
    public <RESULT> NeedsResponseHandler<RESULT> v3( Function<CatchupClientV3,PreparedRequest<RESULT>> v3Request )
    {
        Builder<RESULT> reqBuilder = new Builder<>( channelFuture, log );
        return reqBuilder.v3( v3Request );
    }

    @Override
    public void close()
    {
    }

    private class Builder<RESULT> implements CatchupRequestBuilder<RESULT>
    {
        private final CompletableFuture<CatchupChannel> channel;
        private final Log log;
        private Function<CatchupClientV3,PreparedRequest<RESULT>> v3Request;
        private CatchupResponseCallback<RESULT> responseHandler;

        private Builder( CompletableFuture<CatchupChannel> channel, Log log )
        {
            this.channel = channel;
            this.log = log;
        }

        @Override
        public NeedsResponseHandler<RESULT> v3( Function<CatchupClientV3,PreparedRequest<RESULT>> v3Request )
        {
            this.v3Request = v3Request;
            return this;
        }

        @Override
        public CatchupRequestBuilder<RESULT> withResponseHandler( CatchupResponseCallback<RESULT> responseHandler )
        {
            this.responseHandler = responseHandler;
            return this;
        }

        @Override
        public RESULT request() throws Exception
        {
            return channel
                    .thenCompose( this::performRequest )
                    .get( REQUEST_SENT_TIMEOUT, REQUEST_SENT_TIME_UNIT )
                    .get();
        }

        private CompletableFuture<OperationProgressMonitor<RESULT>> performRequest( CatchupChannel catchupChannel )
        {
            return catchupChannel.protocol().thenApply( protocol -> performRequest( protocol, catchupChannel ) );
        }

        private OperationProgressMonitor<RESULT> performRequest( ApplicationProtocol protocol, CatchupChannel catchupChannel )
        {
            if ( protocol.equals( ApplicationProtocols.CATCHUP_3 ) )
            {
                CatchupClient.V3 client = new CatchupClient.V3( catchupChannel );
                return performRequest( client, v3Request, protocol, catchupChannel );
            }
            else
            {
                String message = "Unrecognised protocol " + protocol;
                log.error( message );
                throw new IllegalStateException( message );
            }
        }

        private <CLIENT> OperationProgressMonitor<RESULT> performRequest( CLIENT client,
                Function<CLIENT,PreparedRequest<RESULT>> specificVersionRequest,
                ApplicationProtocol protocol, CatchupChannel catchupChannel )
        {
            if ( specificVersionRequest != null )
            {
                PreparedRequest<RESULT> request = specificVersionRequest.apply( client );
                return withProgressMonitor( request.execute( responseHandler ), catchupChannel );
            }
            else
            {
                String message = "No action specified for protocol " + protocol;
                log.error( message );
                throw new IllegalStateException( message );
            }
        }

        private OperationProgressMonitor<RESULT> withProgressMonitor( CompletableFuture<RESULT> request, CatchupChannel catchupChannel )
        {
            return OperationProgressMonitor.of( request, inactivityTimeout.toMillis(), catchupChannel::millisSinceLastResponse, log );
        }
    }

    private static class V3 implements CatchupClientV3
    {
        private final CatchupChannel channel;

        private V3( CatchupChannel channel )
        {
            this.channel = channel;
        }

        @Override
        public PreparedRequest<CoreSnapshot> getCoreSnapshot( DatabaseId databaseId )
        {
            return handler -> makeBlockingRequest( new CoreSnapshotRequest( databaseId ), handler, channel );
        }

        @Override
        public PreparedRequest<StoreId> getStoreId( DatabaseId databaseId )
        {
            return handler -> makeBlockingRequest( new GetStoreIdRequest( databaseId ), handler, channel );
        }

        @Override
        public PreparedRequest<TxStreamFinishedResponse> pullTransactions( StoreId storeId, long previousTxId, DatabaseId databaseId )
        {
            return handler -> makeBlockingRequest( new TxPullRequest( previousTxId, storeId, databaseId ), handler, channel );
        }

        @Override
        public PreparedRequest<PrepareStoreCopyResponse> prepareStoreCopy( StoreId storeId, DatabaseId databaseId )
        {
            return handler -> makeBlockingRequest( new PrepareStoreCopyRequest( storeId, databaseId ), handler, channel );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getStoreFile( StoreId storeId, File file, long requiredTxId, DatabaseId databaseId )
        {
            return handler -> makeBlockingRequest( new GetStoreFileRequest( storeId, file, requiredTxId, databaseId ), handler, channel );
        }

    }

    private static class ReleaseOnComplete implements BiConsumer<Object,Throwable>
    {
        private final CatchupChannel catchUpChannel;

        ReleaseOnComplete( CatchupChannel catchUpChannel )
        {
            this.catchUpChannel = catchUpChannel;
        }

        @Override
        public void accept( Object o, Throwable throwable )
        {
            // we do not care to block for release to finish.
            if ( throwable != null )
            {
                catchUpChannel.dispose();
            }
            else
            {
                catchUpChannel.release();
            }
        }
    }

}
