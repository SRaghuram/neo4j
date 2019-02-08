/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.catchup.v1.storecopy.GetIndexFilesRequest;
import com.neo4j.causalclustering.catchup.v1.storecopy.GetStoreFileRequest;
import com.neo4j.causalclustering.catchup.v1.storecopy.GetStoreIdRequest;
import com.neo4j.causalclustering.catchup.v1.storecopy.PrepareStoreCopyRequest;
import com.neo4j.causalclustering.catchup.v1.tx.TxPullRequest;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequest;
import com.neo4j.causalclustering.helper.OperationProgressMonitor;
import com.neo4j.causalclustering.identity.StoreId;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocol;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.neo4j.logging.Log;

class CatchupClient implements VersionedCatchupClients
{
    private static final int DEFAULT_TIMEOUT = 1;
    private static final TimeUnit DEFAULT_TIME_UNIT = TimeUnit.SECONDS;
    private final String defaultDatabaseName;
    private final CompletableFuture<CatchupChannel> channelFuture;
    private final Duration inactivityTimeout;
    private final Log log;

    CatchupClient( CompletableFuture<CatchupChannel> channelFuture, String defaultDatabaseName, Duration inactivityTimeout, Log log )
    {
        this.channelFuture = channelFuture;
        this.inactivityTimeout = inactivityTimeout;
        this.defaultDatabaseName = defaultDatabaseName;
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
    public <RESULT> NeedsV2Handler<RESULT> v1( Function<CatchupClientV1,PreparedRequest<RESULT>> v1Request )
    {
        Builder<RESULT> reqBuilder = new Builder<>( channelFuture, defaultDatabaseName, log );
        return reqBuilder.v1( v1Request );
    }

    @Override
    public <RESULT> NeedsResponseHandler<RESULT> any( Function<CatchupClientCommon,PreparedRequest<RESULT>> allVersionsRequest )
    {
        Builder<RESULT> reqBuilder = new Builder<>( channelFuture, defaultDatabaseName, log );
        return reqBuilder.any( allVersionsRequest );
    }

    @Override
    public void close()
    {
    }

    private class Builder<RESULT> implements CatchupRequestBuilder<RESULT>
    {
        private final CompletableFuture<CatchupChannel> channel;
        private final String defaultDatabaseName;
        private final Log log;
        private Function<CatchupClientV1,PreparedRequest<RESULT>> v1Request;
        private Function<CatchupClientV2,PreparedRequest<RESULT>> v2Request;
        private Function<CatchupClientV3,PreparedRequest<RESULT>> v3Request;
        private Function<CatchupClientCommon,PreparedRequest<RESULT>> allVersionsRequest;
        private CatchupResponseCallback<RESULT> responseHandler;

        private Builder( CompletableFuture<CatchupChannel> channel, String defaultDatabaseName, Log log )
        {
            this.channel = channel;
            this.defaultDatabaseName = defaultDatabaseName;
            this.log = log;
        }

        @Override
        public NeedsV2Handler<RESULT> v1( Function<CatchupClientV1,PreparedRequest<RESULT>> v1Request )
        {
            this.v1Request = v1Request;
            return this;
        }

        @Override
        public NeedsV3Handler<RESULT> v2( Function<CatchupClientV2,PreparedRequest<RESULT>> v2Request )
        {
            this.v2Request = v2Request;
            return this;
        }

        @Override
        public NeedsResponseHandler<RESULT> v3( Function<CatchupClientV3,PreparedRequest<RESULT>> v3Request )
        {
            this.v3Request = v3Request;
            return this;
        }

        @Override
        public NeedsResponseHandler<RESULT> any( Function<CatchupClientCommon,PreparedRequest<RESULT>> allVersionsRequest )
        {
            this.allVersionsRequest = allVersionsRequest;
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
                    .thenCompose( this::doRequest )
                    .get( DEFAULT_TIMEOUT, DEFAULT_TIME_UNIT )
                    .get();
        }

        private CompletableFuture<OperationProgressMonitor<RESULT>> doRequest( CatchupChannel catchupChannel )
        {
            return catchupChannel.protocol().thenApply( protocol -> doRequest( protocol, catchupChannel ) );
        }

        private OperationProgressMonitor<RESULT> doRequest( ApplicationProtocol protocol, CatchupChannel catchupChannel )
        {
            if ( protocol.equals( Protocol.ApplicationProtocols.CATCHUP_1 ) )
            {
                CatchupClient.V1 client = new CatchupClient.V1( catchupChannel, defaultDatabaseName );
                return performRequest( client, v1Request, allVersionsRequest, protocol, catchupChannel );
            }
            else if ( protocol.equals( Protocol.ApplicationProtocols.CATCHUP_2 ) )
            {
                CatchupClient.V2 client = new CatchupClient.V2( catchupChannel );
                return performRequest( client, v2Request, allVersionsRequest, protocol, catchupChannel );
            }
            else if ( protocol.equals( Protocol.ApplicationProtocols.CATCHUP_3 ) )
            {
                CatchupClient.V3 client = new CatchupClient.V3( catchupChannel );
                return performRequest( client, v3Request, allVersionsRequest, protocol, catchupChannel );
            }
            else
            {
                String message = "Unrecognised protocol " + protocol;
                log.error( message );
                throw new IllegalStateException( message );
            }
        }

        private <CLIENT extends CatchupClientCommon> OperationProgressMonitor<RESULT> performRequest( CLIENT client,
                Function<CLIENT,PreparedRequest<RESULT>> specificVersionRequest,
                Function<CatchupClientCommon,PreparedRequest<RESULT>> allVersionsRequest,
                ApplicationProtocol protocol, CatchupChannel catchupChannel )
        {
            PreparedRequest<RESULT> request;
            if ( specificVersionRequest != null )
            {
                request = specificVersionRequest.apply( client );
            }
            else if ( allVersionsRequest != null )
            {
                request = allVersionsRequest.apply( client );
            }
            else
            {
                String message = "No action specified for protocol " + protocol;
                log.error( message );
                throw new IllegalStateException( message );
            }

            return withProgressMonitor( request.execute( responseHandler ), catchupChannel );
        }

        private OperationProgressMonitor<RESULT> withProgressMonitor( CompletableFuture<RESULT> request, CatchupChannel catchupChannel )
        {
            return OperationProgressMonitor.of( request, inactivityTimeout.toMillis(), catchupChannel::millisSinceLastResponse, log );
        }
    }

    private static class V1 implements CatchupClientV1
    {
        private final CatchupChannel channel;
        private final String defaultDatabaseName;

        V1( CatchupChannel channel, String defaultDatabaseName )
        {
            this.channel = channel;
            this.defaultDatabaseName = defaultDatabaseName;
        }

        @Override
        public PreparedRequest<CoreSnapshot> getCoreSnapshot()
        {
            return handler -> makeBlockingRequest( new CoreSnapshotRequest(), handler, channel );
        }

        @Override
        public PreparedRequest<StoreId> getStoreId()
        {
            return handler -> makeBlockingRequest( new GetStoreIdRequest( defaultDatabaseName ), handler, channel );
        }

        @Override
        public PreparedRequest<TxStreamFinishedResponse> pullTransactions( StoreId storeId, long previousTxId )
        {
            return handler -> makeBlockingRequest( new TxPullRequest( previousTxId, storeId, defaultDatabaseName ), handler, channel );
        }

        @Override
        public PreparedRequest<PrepareStoreCopyResponse> prepareStoreCopy( StoreId storeId )
        {
            return handler -> makeBlockingRequest( new PrepareStoreCopyRequest( storeId, defaultDatabaseName ), handler, channel );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getIndexFiles( StoreId storeId, long indexId, long requiredTxId )
        {
            return handler -> makeBlockingRequest( new GetIndexFilesRequest( storeId, indexId, requiredTxId, defaultDatabaseName ), handler, channel );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getStoreFile( StoreId storeId, File file, long requiredTxId )
        {
            return handler -> makeBlockingRequest( new GetStoreFileRequest( storeId, file, requiredTxId, defaultDatabaseName ), handler, channel );
        }

    }

    private static class V2 implements CatchupClientV2
    {
        private final CatchupChannel channel;

        private V2( CatchupChannel channel )
        {
            this.channel = channel;
        }

        @Override
        public PreparedRequest<CoreSnapshot> getCoreSnapshot()
        {
            return handler -> makeBlockingRequest( new CoreSnapshotRequest(), handler, channel );
        }

        @Override
        public PreparedRequest<StoreId> getStoreId( String databaseName )
        {
            return handler -> makeBlockingRequest( new GetStoreIdRequest( databaseName ), handler, channel );
        }

        @Override
        public PreparedRequest<TxStreamFinishedResponse> pullTransactions( StoreId storeId, long previousTxId, String databaseName )
        {
            return handler -> makeBlockingRequest( new TxPullRequest( previousTxId, storeId, databaseName ), handler, channel );
        }

        @Override
        public PreparedRequest<PrepareStoreCopyResponse> prepareStoreCopy( StoreId storeId, String databaseName )
        {
            return handler -> makeBlockingRequest( new PrepareStoreCopyRequest( storeId, databaseName ), handler, channel );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getIndexFiles( StoreId storeId, long indexId, long requiredTxId, String databaseName )
        {
            return handler -> makeBlockingRequest( new GetIndexFilesRequest( storeId, indexId, requiredTxId, databaseName ), handler, channel );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getStoreFile( StoreId storeId, File file, long requiredTxId, String databaseName )
        {
            return handler -> makeBlockingRequest( new GetStoreFileRequest( storeId, file, requiredTxId, databaseName ), handler, channel );
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
        public PreparedRequest<CoreSnapshot> getCoreSnapshot()
        {
            return handler -> makeBlockingRequest( new CoreSnapshotRequest(), handler, channel );
        }

        @Override
        public PreparedRequest<StoreId> getStoreId( String databaseName )
        {
            return handler -> makeBlockingRequest( new GetStoreIdRequest( databaseName ), handler, channel );
        }

        @Override
        public PreparedRequest<TxStreamFinishedResponse> pullTransactions( StoreId storeId, long previousTxId, String databaseName )
        {
            return handler -> makeBlockingRequest( new TxPullRequest( previousTxId, storeId, databaseName ), handler, channel );
        }

        @Override
        public PreparedRequest<PrepareStoreCopyResponse> prepareStoreCopy( StoreId storeId, String databaseName )
        {
            return handler -> makeBlockingRequest( new PrepareStoreCopyRequest( storeId, databaseName ), handler, channel );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getIndexFiles( StoreId storeId, long indexId, long requiredTxId, String databaseName )
        {
            return handler -> makeBlockingRequest( new GetIndexFilesRequest( storeId, indexId, requiredTxId, databaseName ), handler, channel );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getStoreFile( StoreId storeId, File file, long requiredTxId, String databaseName )
        {
            return handler -> makeBlockingRequest( new GetStoreFileRequest( storeId, file, requiredTxId, databaseName ), handler, channel );
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
            catchUpChannel.release();
        }
    }

}
