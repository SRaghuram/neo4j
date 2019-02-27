/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import java.io.File;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import org.neo4j.causalclustering.catchup.v1.storecopy.GetIndexFilesRequest;
import org.neo4j.causalclustering.catchup.v1.storecopy.GetStoreFileRequest;
import org.neo4j.causalclustering.catchup.v1.storecopy.GetStoreIdRequest;
import org.neo4j.causalclustering.catchup.v1.storecopy.PrepareStoreCopyRequest;
import org.neo4j.causalclustering.catchup.v1.tx.TxPullRequest;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequest;
import org.neo4j.causalclustering.helper.TimeoutRetrier;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocol;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.Log;

class CatchupClient implements VersionedCatchupClients
{
    private final CatchupClientFactory.CatchupChannel channel;
    private final ApplicationProtocol protocol;
    private final CatchupChannelPool<CatchupClientFactory.CatchupChannel> pool;
    private final String defaultDatabaseName;
    private final Duration inactivityTimeout;

    CatchupClient( AdvertisedSocketAddress upstream, CatchupChannelPool<CatchupClientFactory.CatchupChannel> pool, String defaultDatabaseName,
            Duration inactivityTimeout ) throws Exception
    {
        this.inactivityTimeout = inactivityTimeout;
        this.channel = pool.acquire( upstream );
        this.protocol = channel.protocol().get();
        this.pool = pool;
        this.defaultDatabaseName = defaultDatabaseName;
    }

    private static <RESULT> CompletableFuture<RESULT> makeBlockingRequest( CatchupProtocolMessage request,
            CatchupResponseCallback<RESULT> responseHandler,
            CatchupClientFactory.CatchupChannel channel,
            CatchupChannelPool<CatchupClientFactory.CatchupChannel> pool )
    {
        CompletableFuture<RESULT> future = new CompletableFuture<>();
        try
        {
            future.whenComplete( new ReleaseOnComplete( channel, pool ) );

            channel.setResponseHandler( responseHandler, future );
            channel.send( request );
        }
        catch ( Exception e )
        {
            future.completeExceptionally( new CatchUpClientException( "Failed to send request", e ) );
            if ( channel != null )
            {
                pool.dispose( channel );
            }
        }

        return future;
    }

    @Override
    public <RESULT> NeedsV2Handler<RESULT> v1( Function<CatchupClientV1,PreparedRequest<RESULT>> v1Request )
    {
        Builder<RESULT> reqBuilder = new Builder<>( channel, pool, protocol, defaultDatabaseName );
        return reqBuilder.v1( v1Request );
    }

    @Override
    public <RESULT> NeedsResponseHandler<RESULT> any( Function<CatchupClientCommon,PreparedRequest<RESULT>> allVersionsRequest )
    {
        Builder<RESULT> reqBuilder = new Builder<>( channel, pool, protocol, defaultDatabaseName );
        return reqBuilder.any( allVersionsRequest );
    }

    @Override
    public ApplicationProtocol protocol()
    {
        return protocol;
    }

    @Override
    public void close()
    {
        pool.release( channel );
    }

    private class Builder<RESULT> implements CatchupRequestBuilder<RESULT>
    {
        private final CatchupClientFactory.CatchupChannel channel;
        private final CatchupChannelPool<CatchupClientFactory.CatchupChannel> pool;
        private final ApplicationProtocol protocol;
        private final String defaultDatabaseName;
        private Function<CatchupClientV1,PreparedRequest<RESULT>> v1Request;
        private Function<CatchupClientV2,PreparedRequest<RESULT>> v2Request;
        private Function<CatchupClientCommon,PreparedRequest<RESULT>> allVersionsRequest;
        private CatchupResponseCallback<RESULT> responseHandler;

        private Builder( CatchupClientFactory.CatchupChannel channel, CatchupChannelPool<CatchupClientFactory.CatchupChannel> pool,
                ApplicationProtocol protocol, String defaultDatabaseName )
        {
            this.channel = channel;
            this.pool = pool;
            this.protocol = protocol;
            this.defaultDatabaseName = defaultDatabaseName;
        }

        @Override
        public NeedsV2Handler<RESULT> v1( Function<CatchupClientV1,PreparedRequest<RESULT>> v1Request )
        {
            this.v1Request = v1Request;
            return this;
        }

        @Override
        public NeedsResponseHandler<RESULT> v2( Function<CatchupClientV2,PreparedRequest<RESULT>> v2Request )
        {
            this.v2Request = v2Request;
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
        public RESULT request( Log log ) throws Exception
        {
            if ( protocol.equals( ApplicationProtocols.CATCHUP_1 ) )
            {
                CatchupClient.V1 client = new CatchupClient.V1( channel, pool, defaultDatabaseName );
                return timeoutRetrier( log, client, v1Request, allVersionsRequest, protocol );
            }
            else if ( protocol.equals( ApplicationProtocols.CATCHUP_2 ) )
            {
                CatchupClient.V2 client = new CatchupClient.V2( channel, pool );
                return timeoutRetrier( log, client, v2Request, allVersionsRequest, protocol );
            }
            else
            {
                String message = "Unrecognised protocol " + protocol;
                log.error( message );
                throw new Exception( message );
            }
        }

        private <CLIENT extends CatchupClientCommon> RESULT timeoutRetrier( Log log, CLIENT client,
                Function<CLIENT,PreparedRequest<RESULT>> specificVersionRequest, Function<CatchupClientCommon,PreparedRequest<RESULT>> allVersionsRequest,
                ApplicationProtocol protocol ) throws Exception
        {
            if ( specificVersionRequest != null )
            {
                return retrying( specificVersionRequest.apply( client ).execute( responseHandler ) ).get( log );
            }
            else if ( allVersionsRequest != null )
            {
                return retrying( allVersionsRequest.apply( client ).execute( responseHandler ) ).get( log );
            }
            else
            {
                String message = "No action specified for protocol " + protocol;
                log.error( message );
                throw new Exception( message );
            }
        }

        private TimeoutRetrier<RESULT> retrying( CompletableFuture<RESULT> request )
        {
            return TimeoutRetrier.of( request, inactivityTimeout.toMillis(), channel::millisSinceLastResponse );
        }
    }

    private static class V1 implements CatchupClientV1
    {
        private final CatchupClientFactory.CatchupChannel channel;
        private final CatchupChannelPool<CatchupClientFactory.CatchupChannel> pool;
        private final String defaultDatabaseName;

        V1( CatchupClientFactory.CatchupChannel channel, CatchupChannelPool<CatchupClientFactory.CatchupChannel> pool, String defaultDatabaseName )
        {
            this.channel = channel;
            this.pool = pool;
            this.defaultDatabaseName = defaultDatabaseName;
        }

        @Override
        public PreparedRequest<CoreSnapshot> getCoreSnapshot()
        {
            return handler -> makeBlockingRequest( new CoreSnapshotRequest(), handler, channel, pool );
        }

        @Override
        public PreparedRequest<StoreId> getStoreId()
        {
            return handler -> makeBlockingRequest( new GetStoreIdRequest( defaultDatabaseName ), handler, channel, pool );
        }

        @Override
        public PreparedRequest<TxStreamFinishedResponse> pullTransactions( StoreId storeId, long previousTxId )
        {
            return handler -> makeBlockingRequest( new TxPullRequest( previousTxId, storeId, defaultDatabaseName ), handler, channel, pool );
        }

        @Override
        public PreparedRequest<PrepareStoreCopyResponse> prepareStoreCopy( StoreId storeId )
        {
            return handler -> makeBlockingRequest( new PrepareStoreCopyRequest( storeId, defaultDatabaseName ), handler, channel, pool );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getIndexFiles( StoreId storeId, long indexId, long requiredTxId )
        {
            return handler -> makeBlockingRequest( new GetIndexFilesRequest( storeId, indexId, requiredTxId, defaultDatabaseName ), handler, channel, pool );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getStoreFile( StoreId storeId, File file, long requiredTxId )
        {
            return handler -> makeBlockingRequest( new GetStoreFileRequest( storeId, file, requiredTxId, defaultDatabaseName ), handler, channel, pool );
        }

    }

    private static class V2 implements CatchupClientV2
    {
        private final CatchupClientFactory.CatchupChannel channel;
        private final CatchupChannelPool<CatchupClientFactory.CatchupChannel> pool;

        private V2( CatchupClientFactory.CatchupChannel channel, CatchupChannelPool<CatchupClientFactory.CatchupChannel> pool )
        {
            this.channel = channel;
            this.pool = pool;
        }

        @Override
        public PreparedRequest<CoreSnapshot> getCoreSnapshot()
        {
            return handler -> makeBlockingRequest( new CoreSnapshotRequest(), handler, channel, pool );
        }

        @Override
        public PreparedRequest<StoreId> getStoreId( String databaseName )
        {
            return handler -> makeBlockingRequest( new GetStoreIdRequest( databaseName ), handler, channel, pool );
        }

        @Override
        public PreparedRequest<TxStreamFinishedResponse> pullTransactions( StoreId storeId, long previousTxId, String databaseName )
        {
            return handler -> makeBlockingRequest( new TxPullRequest( previousTxId, storeId, databaseName ), handler, channel, pool );
        }

        @Override
        public PreparedRequest<PrepareStoreCopyResponse> prepareStoreCopy( StoreId storeId, String databaseName )
        {
            return handler -> makeBlockingRequest( new PrepareStoreCopyRequest( storeId, databaseName ), handler, channel, pool );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getIndexFiles( StoreId storeId, long indexId, long requiredTxId, String databaseName )
        {
            return handler -> makeBlockingRequest( new GetIndexFilesRequest( storeId, indexId, requiredTxId, databaseName ), handler, channel, pool );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getStoreFile( StoreId storeId, File file, long requiredTxId, String databaseName )
        {
            return handler -> makeBlockingRequest( new GetStoreFileRequest( storeId, file, requiredTxId, databaseName ), handler, channel, pool );
        }

    }

    private static class ReleaseOnComplete implements BiConsumer<Object,Throwable>
    {
        private final CatchupClientFactory.CatchupChannel catchUpChannel;
        private final CatchupChannelPool<CatchupClientFactory.CatchupChannel> pool;

        ReleaseOnComplete( CatchupClientFactory.CatchupChannel catchUpChannel, CatchupChannelPool<CatchupClientFactory.CatchupChannel> pool )
        {
            this.catchUpChannel = catchUpChannel;
            this.pool = pool;
        }

        @Override
        public void accept( Object o, Throwable throwable )
        {
            catchUpChannel.clearResponseHandler();
            if ( throwable == null )
            {
                pool.release( catchUpChannel );
            }
            else
            {
                pool.dispose( catchUpChannel );
            }
        }
    }

}
