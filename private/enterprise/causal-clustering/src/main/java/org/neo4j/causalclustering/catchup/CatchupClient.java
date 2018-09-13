/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import org.neo4j.causalclustering.catchup.tx.TxPullResult;
import org.neo4j.causalclustering.catchup.v1.storecopy.GetIndexFilesRequest;
import org.neo4j.causalclustering.catchup.v1.storecopy.GetStoreFileRequest;
import org.neo4j.causalclustering.catchup.v1.storecopy.GetStoreIdRequest;
import org.neo4j.causalclustering.catchup.v1.storecopy.PrepareStoreCopyRequest;
import org.neo4j.causalclustering.catchup.v1.tx.TxPullRequest;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshotRequest;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocol;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.util.concurrent.Futures;

class CatchupClient implements VersionedCatchupClients
{
    private final CatchupClientFactory.CatchupChannel channel;
    private final ApplicationProtocol protocol;
    private final CatchupChannelPool<CatchupClientFactory.CatchupChannel> pool;
    private final String defaultDatabaseName;

    CatchupClient( AdvertisedSocketAddress upstream, CatchupChannelPool<CatchupClientFactory.CatchupChannel> pool, String defaultDatabaseName ) throws Exception
    {
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
        //TODO: The interface should probably implement Autoclosable still, to account for future alternative implementations, but shouldn't this
        // method be a no-op? We already release the channel from the pool in ReleaseOnComplete.
        //pool.release( channel );
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
        public CompletableFuture<RESULT> request()
        {
            if ( protocol.equals( ApplicationProtocols.CATCHUP_1 ) )
            {
                V1 v1 = new CatchupClient.V1( channel, pool, defaultDatabaseName );
                if ( v1Request != null )
                {
                    return v1Request.apply( v1 ).execute( responseHandler );
                }
                else if ( allVersionsRequest != null )
                {
                    return allVersionsRequest.apply( v1 ).execute( responseHandler );
                }
                else
                {
                    Futures.failedFuture( new Exception( "No V1 action specified" ) );
                }
            }
            else if ( protocol.equals( ApplicationProtocols.CATCHUP_2 ) )
            {
                V2 v2 = new CatchupClient.V2( channel, pool );
                if ( v2Request != null )
                {
                    return v2Request.apply( v2 ).execute( responseHandler );
                }
                else if ( allVersionsRequest != null )
                {
                    return allVersionsRequest.apply( v2 ).execute( responseHandler );
                }
                else
                {
                    return Futures.failedFuture( new Exception( "No V2 action specified" ) );
                }
            }

            return Futures.failedFuture( new Exception( "Unrecognised protocol" ) );
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
        public PreparedRequest<TxPullResult> pullTransactions( StoreId storeId, long previousTxId )
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
        public PreparedRequest<TxPullResult> pullTransactions( StoreId storeId, long previousTxId, String databaseName )
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
