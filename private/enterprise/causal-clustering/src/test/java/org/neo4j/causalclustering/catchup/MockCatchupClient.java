/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import java.io.File;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import org.neo4j.causalclustering.catchup.v1.storecopy.GetIndexFilesRequest;
import org.neo4j.causalclustering.catchup.v1.storecopy.GetStoreFileRequest;
import org.neo4j.causalclustering.catchup.v1.storecopy.GetStoreIdRequest;
import org.neo4j.causalclustering.catchup.v1.storecopy.PrepareStoreCopyRequest;
import org.neo4j.causalclustering.catchup.v1.tx.TxPullRequest;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import org.neo4j.causalclustering.helper.TimeoutRetrier;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocol;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import org.neo4j.logging.Log;
import org.neo4j.util.concurrent.Futures;

import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols.CATCHUP_1;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols.CATCHUP_2;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class MockCatchupClient implements VersionedCatchupClients
{
    private ApplicationProtocol protocol;
    private CatchupClientV1 v1Client;
    private CatchupClientV2 v2Client;

    public MockCatchupClient( ApplicationProtocol protocol, CatchupClientV1 v1Client, CatchupClientV2 v2Client )
    {
        this.protocol = protocol;
        this.v1Client = v1Client;
        this.v2Client = v2Client;
    }

    public static MockClientResponses responses()
    {
        return new MockClientResponses();
    }

    @Override
    public <RESULT> NeedsV2Handler<RESULT> v1( Function<CatchupClientV1,PreparedRequest<RESULT>> v1Request )
    {
        Builder<RESULT> reqBuilder = new Builder<>( v1Client, v2Client );
        return reqBuilder.v1( v1Request );
    }

    @Override
    public <RESULT> NeedsResponseHandler<RESULT> any( Function<CatchupClientCommon,PreparedRequest<RESULT>> allVersionsRequest )
    {
        Builder<RESULT> reqBuilder = new Builder<>( v1Client, v2Client );
        return reqBuilder.any( allVersionsRequest );
    }

    @Override
    public ApplicationProtocol protocol()
    {
        return protocol;
    }

    public void setProtocol( ApplicationProtocol protocol )
    {
        this.protocol = protocol;
    }

    @Override
    public void close() throws Exception
    {
    }

    private class Builder<RESULT> implements CatchupRequestBuilder<RESULT>
    {
        private Function<CatchupClientV1,PreparedRequest<RESULT>> v1Request;
        private Function<CatchupClientV2,PreparedRequest<RESULT>> v2Request;
        private CatchupClientV1 v1Client;
        private CatchupClientV2 v2Client;

        Builder( CatchupClientV1 v1Client, CatchupClientV2 v2Client )
        {
            this.v1Client = v1Client;
            this.v2Client = v2Client;
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
            if ( protocol.equals( CATCHUP_1 ) )
            {
                v1Request = allVersionsRequest::apply;
            }
            else if ( protocol.equals( CATCHUP_2 ) )
            {
                v2Request = allVersionsRequest::apply;
            }
            return this;
        }

        @Override
        public IsPrepared<RESULT> withResponseHandler( CatchupResponseCallback<RESULT> responseHandler )
        {
            //no-op in the mock for now.
            return this;
        }

        @Override
        public RESULT request( Log log ) throws Exception
        {
            if ( protocol.equals( CATCHUP_1 ) )
            {
                return retrying( v1Request.apply( v1Client ).execute( null ) ).get( log );
            }
            else if ( protocol.equals( ApplicationProtocols.CATCHUP_2 ) )
            {
                return retrying( v2Request.apply( v2Client ).execute( null ) ).get( log );
            }
            return retrying( Futures.failedFuture( new Exception( "Unrecognised protocol" ) ) ).get( log );
        }

        private TimeoutRetrier<RESULT> retrying( CompletableFuture<RESULT> request )
        {
            return TimeoutRetrier.of( request, 1, () -> Optional.of( 0L ) );
        }
    }

    public static class MockClientV1 implements CatchupClientV1
    {

        private final MockClientResponses responses;

        public MockClientV1( MockClientResponses responses )
        {
            this.responses = responses;
        }

        @Override
        public PreparedRequest<CoreSnapshot> getCoreSnapshot()
        {
            return handler -> CompletableFuture.completedFuture( responses.coreSnapshot.get() );
        }

        @Override
        public PreparedRequest<StoreId> getStoreId()
        {
            StoreId storeId = responses.storeId.apply( new GetStoreIdRequest( DEFAULT_DATABASE_NAME ) );
            return handler -> CompletableFuture.completedFuture( storeId );
        }

        @Override
        public PreparedRequest<TxStreamFinishedResponse> pullTransactions( StoreId storeId, long previousTxId )
        {
            TxStreamFinishedResponse pullResponse = responses.txPullResponse.apply( new TxPullRequest( previousTxId, storeId, DEFAULT_DATABASE_NAME ) );
            return handler -> CompletableFuture.completedFuture( pullResponse );
        }

        @Override
        public PreparedRequest<PrepareStoreCopyResponse> prepareStoreCopy( StoreId storeId )
        {
            PrepareStoreCopyResponse prepareStoreCopyResponse = responses.prepareStoreCopyResponse
                    .apply( new PrepareStoreCopyRequest( storeId, DEFAULT_DATABASE_NAME ) );
            return handler -> CompletableFuture.completedFuture( prepareStoreCopyResponse );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getIndexFiles( StoreId storeId, long indexId, long requiredTxId )
        {
            StoreCopyFinishedResponse storeCopyFinishedResponse = responses.indexFiles
                    .apply( new GetIndexFilesRequest( storeId, indexId, requiredTxId, DEFAULT_DATABASE_NAME ) );
            return handler -> CompletableFuture.completedFuture( storeCopyFinishedResponse );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getStoreFile( StoreId storeId, File file, long requiredTxId )
        {
            StoreCopyFinishedResponse storeCopyFinishedResponse = responses.storeFiles
                    .apply( new GetStoreFileRequest( storeId, file, requiredTxId, DEFAULT_DATABASE_NAME ) );
            return handler -> CompletableFuture.completedFuture( storeCopyFinishedResponse );
        }
    }

    public static class MockClientV2 implements CatchupClientV2
    {

        private final MockClientResponses responses;

        public MockClientV2( MockClientResponses responses )
        {
            this.responses = responses;
        }

        @Override
        public PreparedRequest<CoreSnapshot> getCoreSnapshot()
        {
            return handler -> CompletableFuture.completedFuture( responses.coreSnapshot.get() );
        }

        @Override
        public PreparedRequest<StoreId> getStoreId( String databaseName )
        {
            StoreId storeId = responses.storeId.apply( new GetStoreIdRequest( databaseName ) );
            return handler -> CompletableFuture.completedFuture( storeId );
        }

        @Override
        public PreparedRequest<TxStreamFinishedResponse> pullTransactions( StoreId storeId, long previousTxId, String databaseName )
        {
             TxStreamFinishedResponse pullResponse = responses.txPullResponse.apply( new TxPullRequest( previousTxId, storeId, databaseName ) );
            return handler -> CompletableFuture.completedFuture( pullResponse );
        }

        @Override
        public PreparedRequest<PrepareStoreCopyResponse> prepareStoreCopy( StoreId storeId, String databaseName )
        {
            PrepareStoreCopyResponse prepareStoreCopyResponse = responses.prepareStoreCopyResponse
                    .apply( new PrepareStoreCopyRequest( storeId, databaseName ) );
            return handler -> CompletableFuture.completedFuture( prepareStoreCopyResponse );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getIndexFiles( StoreId storeId, long indexId, long requiredTxId, String databaseName )
        {
            StoreCopyFinishedResponse storeCopyFinishedResponse = responses.indexFiles
                    .apply( new GetIndexFilesRequest( storeId, indexId, requiredTxId, databaseName ) );
            return handler -> CompletableFuture.completedFuture( storeCopyFinishedResponse );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getStoreFile( StoreId storeId, File file, long requiredTxId, String databaseName )
        {
            StoreCopyFinishedResponse storeCopyFinishedResponse = responses.storeFiles
                    .apply( new GetStoreFileRequest( storeId, file, requiredTxId, databaseName ) );
            return handler -> CompletableFuture.completedFuture( storeCopyFinishedResponse );
        }
    }

    public static class MockClientResponses
    {

        private Supplier<CoreSnapshot> coreSnapshot;
        private Function<GetStoreIdRequest,StoreId> storeId;
        private Function<TxPullRequest,TxStreamFinishedResponse> txPullResponse;
        private Function<PrepareStoreCopyRequest,PrepareStoreCopyResponse> prepareStoreCopyResponse;
        private Function<GetIndexFilesRequest,StoreCopyFinishedResponse> indexFiles;
        private Function<GetStoreFileRequest,StoreCopyFinishedResponse> storeFiles;

        public MockClientResponses withCoreSnapshot( CoreSnapshot coreSnapshot )
        {
            this.coreSnapshot = () -> coreSnapshot;
            return this;
        }

        public MockClientResponses withCoreSnapshot( Supplier<CoreSnapshot> coreSnapshot )
        {
            this.coreSnapshot = coreSnapshot;
            return this;
        }

        public MockClientResponses withStoreId( StoreId storeId )
        {
            this.storeId = ignored -> storeId;
            return this;
        }

        public MockClientResponses withStoreId( Function<GetStoreIdRequest,StoreId> storeId )
        {
            this.storeId = storeId;
            return this;
        }

        public MockClientResponses withTxPullResponse( TxStreamFinishedResponse txPullResponse )
        {
            this.txPullResponse = ignored -> txPullResponse;
            return this;
        }

        public MockClientResponses withTxPullResponse( Function<TxPullRequest,TxStreamFinishedResponse> txPullResponse )
        {
            this.txPullResponse = txPullResponse;
            return this;
        }

        public MockClientResponses withPrepareStoreCopyResponse( PrepareStoreCopyResponse prepareStoreCopyResponse )
        {
            this.prepareStoreCopyResponse = ignored -> prepareStoreCopyResponse;
            return this;
        }

        public MockClientResponses withPrepareStoreCopyResponse( Function<PrepareStoreCopyRequest,PrepareStoreCopyResponse> prepareStoreCopyResponse )
        {
            this.prepareStoreCopyResponse = prepareStoreCopyResponse;
            return this;
        }

        public MockClientResponses withIndexFilesResponse( StoreCopyFinishedResponse indexFiles )
        {
            this.indexFiles = ignored -> indexFiles;
            return this;
        }

        public MockClientResponses withIndexFilesResponse( Function<GetIndexFilesRequest,StoreCopyFinishedResponse> indexFiles )
        {
            this.indexFiles = indexFiles;
            return this;
        }

        public MockClientResponses withStoreFilesResponse( StoreCopyFinishedResponse storeFiles )
        {
            this.storeFiles = ignored -> storeFiles;
            return this;
        }

        public MockClientResponses withStoreFilesResponse( Function<GetStoreFileRequest,StoreCopyFinishedResponse> storeFiles )
        {
            this.storeFiles = storeFiles;
            return this;
        }
    }
}
