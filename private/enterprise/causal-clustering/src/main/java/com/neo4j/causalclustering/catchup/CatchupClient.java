/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdRequest;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreFileRequest;
import com.neo4j.causalclustering.catchup.v3.storecopy.GetStoreIdRequest;
import com.neo4j.causalclustering.catchup.v3.storecopy.PrepareStoreCopyRequest;
import com.neo4j.causalclustering.catchup.v3.tx.TxPullRequest;
import com.neo4j.causalclustering.catchup.v4.databases.GetAllDatabaseIdsRequest;
import com.neo4j.causalclustering.catchup.v4.databases.GetAllDatabaseIdsResponse;
import com.neo4j.causalclustering.catchup.v4.info.InfoRequest;
import com.neo4j.causalclustering.catchup.v4.info.InfoResponse;
import com.neo4j.causalclustering.catchup.v4.metadata.GetMetadataRequest;
import com.neo4j.causalclustering.catchup.v4.metadata.GetMetadataResponse;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequest;
import com.neo4j.causalclustering.helper.OperationProgressMonitor;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.neo4j.kernel.database.NamedDatabaseId;
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
    public <RESULT> NeedsV4Handler<RESULT> v3( Function<CatchupClientV3,PreparedRequest<RESULT>> v3Request )
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
        private Function<CatchupClientV4,PreparedRequest<RESULT>> v4Request;
        private Function<CatchupClientV5,PreparedRequest<RESULT>> v5Request;
        private CatchupResponseCallback<RESULT> responseHandler;

        private Builder( CompletableFuture<CatchupChannel> channel, Log log )
        {
            this.channel = channel;
            this.log = log;
        }

        @Override
        public NeedsV4Handler<RESULT> v3( Function<CatchupClientV3,PreparedRequest<RESULT>> v3Request )
        {
            this.v3Request = v3Request;
            return this;
        }

        @Override
        public NeedsV5Handler<RESULT> v4( Function<CatchupClientV4,PreparedRequest<RESULT>> v4Request )
        {
            this.v4Request = v4Request;
            return this;
        }

        @Override
        public NeedsResponseHandler<RESULT> v5( Function<CatchupClientV5,PreparedRequest<RESULT>> v5Request )
        {
            this.v5Request = v5Request;
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
                    .thenApply( this::performRequest )
                    .get( REQUEST_SENT_TIMEOUT, REQUEST_SENT_TIME_UNIT )
                    .get();
        }

        private OperationProgressMonitor<RESULT> performRequest( CatchupChannel catchupChannel )
        {
            var protocol = catchupChannel.protocol();

            if ( protocol.equals( ApplicationProtocols.CATCHUP_3_0 ) )
            {
                CatchupClient.V3 client = new CatchupClient.V3( catchupChannel );
                return performRequest( client, v3Request, protocol, catchupChannel );
            }
            else if ( protocol.equals( ApplicationProtocols.CATCHUP_4_0 ) )
            {
                CatchupClient.V4 client = new CatchupClient.V4( catchupChannel );
                return performRequest( client, v4Request, protocol, catchupChannel );
            }
            else if ( protocol.equals( ApplicationProtocols.CATCHUP_5_0 ) )
            {
                CatchupClient.V5 client = new CatchupClient.V5( catchupChannel );
                return performRequest( client, v5Request, protocol, catchupChannel );
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
        public PreparedRequest<NamedDatabaseId> getDatabaseId( String databaseName )
        {
            return handler -> makeBlockingRequest( new GetDatabaseIdRequest( databaseName ), handler, channel );
        }

        @Override
        public PreparedRequest<CoreSnapshot> getCoreSnapshot( NamedDatabaseId namedDatabaseId )
        {
            return handler -> makeBlockingRequest( new CoreSnapshotRequest( namedDatabaseId.databaseId() ), handler, channel );
        }

        @Override
        public PreparedRequest<StoreId> getStoreId( NamedDatabaseId namedDatabaseId )
        {
            return handler -> makeBlockingRequest( new GetStoreIdRequest( namedDatabaseId.databaseId() ), handler, channel );
        }

        @Override
        public PreparedRequest<TxStreamFinishedResponse> pullTransactions( StoreId storeId, long previousTxId, NamedDatabaseId namedDatabaseId )
        {
            return handler -> makeBlockingRequest( new TxPullRequest( previousTxId, storeId, namedDatabaseId.databaseId() ), handler, channel );
        }

        @Override
        public PreparedRequest<PrepareStoreCopyResponse> prepareStoreCopy( StoreId storeId, NamedDatabaseId namedDatabaseId )
        {
            return handler -> makeBlockingRequest( new PrepareStoreCopyRequest( storeId, namedDatabaseId.databaseId() ), handler, channel );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getStoreFile( StoreId storeId, Path file, long requiredTxId, NamedDatabaseId namedDatabaseId )
        {
            return handler -> makeBlockingRequest( new GetStoreFileRequest( storeId, file, requiredTxId, namedDatabaseId.databaseId() ), handler, channel );
        }
    }

    private static class V4 implements CatchupClientV4
    {

        private final V3 v3;
        private final CatchupChannel channel;

        V4( CatchupChannel channel )
        {
            this.v3 = new V3( channel );
            this.channel = channel;
        }

        @Override
        public PreparedRequest<NamedDatabaseId> getDatabaseId( String databaseName )
        {
            return v3.getDatabaseId( databaseName );
        }

        @Override
        public PreparedRequest<CoreSnapshot> getCoreSnapshot( NamedDatabaseId namedDatabaseId )
        {
            return v3.getCoreSnapshot( namedDatabaseId );
        }

        @Override
        public PreparedRequest<StoreId> getStoreId( NamedDatabaseId namedDatabaseId )
        {
            return v3.getStoreId( namedDatabaseId );
        }

        @Override
        public PreparedRequest<TxStreamFinishedResponse> pullTransactions( StoreId storeId, long previousTxId, NamedDatabaseId namedDatabaseId )
        {
            return v3.pullTransactions( storeId, previousTxId, namedDatabaseId );
        }

        @Override
        public PreparedRequest<PrepareStoreCopyResponse> prepareStoreCopy( StoreId storeId, NamedDatabaseId namedDatabaseId )
        {
            return v3.prepareStoreCopy( storeId, namedDatabaseId );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getStoreFile( StoreId storeId, Path path, long requiredTxId, NamedDatabaseId namedDatabaseId )
        {
            return v3.getStoreFile( storeId, path, requiredTxId, namedDatabaseId );
        }

        @Override
        public PreparedRequest<GetAllDatabaseIdsResponse> getAllDatabaseIds()
        {
            return handler -> makeBlockingRequest( new GetAllDatabaseIdsRequest(), handler, channel );
        }

        @Override
        public PreparedRequest<InfoResponse> getReconciledInfo( NamedDatabaseId namedDatabaseId )
        {
            return handler -> makeBlockingRequest( new InfoRequest( namedDatabaseId ), handler, channel );
        }

        @Override
        public PreparedRequest<GetMetadataResponse> getMetadata( String databaseName, String includeMetadata )
        {
            return handler -> makeBlockingRequest( new GetMetadataRequest( databaseName, includeMetadata ), handler, channel );
        }
    }

    private static class V5 implements CatchupClientV5
    {

        private final V4 v4;

        V5( CatchupChannel channel )
        {
            this.v4 = new V4( channel );
        }

        @Override
        public PreparedRequest<NamedDatabaseId> getDatabaseId( String databaseName )
        {
            return v4.getDatabaseId( databaseName );
        }

        @Override
        public PreparedRequest<CoreSnapshot> getCoreSnapshot( NamedDatabaseId namedDatabaseId )
        {
            return v4.getCoreSnapshot( namedDatabaseId );
        }

        @Override
        public PreparedRequest<StoreId> getStoreId( NamedDatabaseId namedDatabaseId )
        {
            return v4.getStoreId( namedDatabaseId );
        }

        @Override
        public PreparedRequest<TxStreamFinishedResponse> pullTransactions( StoreId storeId, long previousTxId, NamedDatabaseId namedDatabaseId )
        {
            return v4.pullTransactions( storeId, previousTxId, namedDatabaseId );
        }

        @Override
        public PreparedRequest<PrepareStoreCopyResponse> prepareStoreCopy( StoreId storeId, NamedDatabaseId namedDatabaseId )
        {
            return v4.prepareStoreCopy( storeId, namedDatabaseId );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getStoreFile( StoreId storeId, Path path, long requiredTxId, NamedDatabaseId namedDatabaseId )
        {
            return v4.getStoreFile( storeId, path, requiredTxId, namedDatabaseId );
        }

        @Override
        public PreparedRequest<GetAllDatabaseIdsResponse> getAllDatabaseIds()
        {
            return v4.getAllDatabaseIds();
        }

        @Override
        public PreparedRequest<InfoResponse> getReconciledInfo( NamedDatabaseId databaseId )
        {
            return v4.getReconciledInfo( databaseId );
        }

        @Override
        public PreparedRequest<GetMetadataResponse> getMetadata( String databaseName, String includeMetadata )
        {
            return v4.getMetadata( databaseName, includeMetadata );
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
