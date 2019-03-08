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
import com.neo4j.causalclustering.helper.OperationProgressMonitor;
import com.neo4j.causalclustering.identity.StoreId;
import com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;

import java.io.File;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.util.concurrent.Futures;

import static com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols.CATCHUP_1;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class MockCatchupClient implements VersionedCatchupClients
{
    private ApplicationProtocol protocol;
    private CatchupClientV1 v1Client;
    private CatchupClientV2 v2Client;
    private final CatchupClientV3 v3Client;

    public MockCatchupClient( ApplicationProtocol protocol, CatchupClientV1 v1Client, CatchupClientV2 v2Client, CatchupClientV3 v3Client )
    {
        this.protocol = protocol;
        this.v1Client = v1Client;
        this.v2Client = v2Client;
        this.v3Client = v3Client;
    }

    public static MockClientResponses responses()
    {
        return new MockClientResponses();
    }

    @Override
    public <RESULT> NeedsV2Handler<RESULT> v1( Function<CatchupClientV1,PreparedRequest<RESULT>> v1Request )
    {
        Builder<RESULT> reqBuilder = new Builder<>( v1Client, v2Client, v3Client );
        return reqBuilder.v1( v1Request );
    }

    public ApplicationProtocol protocol()
    {
        return protocol;
    }

    public void setProtocol( ApplicationProtocol protocol )
    {
        this.protocol = protocol;
    }

    @Override
    public void close()
    {
    }

    private class Builder<RESULT> implements CatchupRequestBuilder<RESULT>
    {
        private Function<CatchupClientV1,PreparedRequest<RESULT>> v1Request;
        private Function<CatchupClientV2,PreparedRequest<RESULT>> v2Request;
        private Function<CatchupClientV3,PreparedRequest<RESULT>> v3Request;
        private CatchupClientV1 v1Client;
        private CatchupClientV2 v2Client;
        private CatchupClientV3 v3Client;
        private Log log = NullLog.getInstance();

        Builder( CatchupClientV1 v1Client, CatchupClientV2 v2Client, CatchupClientV3 v3Client )
        {
            this.v1Client = v1Client;
            this.v2Client = v2Client;
            this.v3Client = v3Client;
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
        public IsPrepared<RESULT> withResponseHandler( CatchupResponseCallback<RESULT> responseHandler )
        {
            //no-op in the mock for now.
            return this;
        }

        @Override
        public RESULT request() throws Exception
        {
            if ( protocol.equals( CATCHUP_1 ) )
            {
                return withProgressMonitor( v1Request.apply( v1Client ).execute( null ) ).get();
            }
            else if ( protocol.equals( ApplicationProtocols.CATCHUP_2 ) )
            {
                return withProgressMonitor( v2Request.apply( v2Client ).execute( null ) ).get();
            }
            else if ( protocol.equals( ApplicationProtocols.CATCHUP_3 ) )
            {
                return withProgressMonitor( v3Request.apply( v3Client ).execute( null ) ).get();
            }
            return withProgressMonitor( Futures.failedFuture( new Exception( "Unrecognised protocol" ) ) ).get();
        }

        private OperationProgressMonitor<RESULT> withProgressMonitor( CompletableFuture<RESULT> request )
        {
            return OperationProgressMonitor.of( request, 1, () -> OptionalLong.of( 0L ), log );
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

    public static class MockClientV3 implements CatchupClientV3
    {

        private final MockClientResponses responses;

        public MockClientV3( MockClientResponses responses )
        {
            this.responses = responses;
        }

        @Override
        public PreparedRequest<CoreSnapshot> getCoreSnapshot( String databaseName )
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
            PrepareStoreCopyResponse prepareStoreCopyResponse =
                    responses.prepareStoreCopyResponse.apply( new PrepareStoreCopyRequest( storeId, databaseName ) );
            return handler -> CompletableFuture.completedFuture( prepareStoreCopyResponse );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getIndexFiles( StoreId storeId, long indexId, long requiredTxId, String databaseName )
        {
            StoreCopyFinishedResponse storeCopyFinishedResponse =
                    responses.indexFiles.apply( new GetIndexFilesRequest( storeId, indexId, requiredTxId, databaseName ) );
            return handler -> CompletableFuture.completedFuture( storeCopyFinishedResponse );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getStoreFile( StoreId storeId, File file, long requiredTxId, String databaseName )
        {
            StoreCopyFinishedResponse storeCopyFinishedResponse =
                    responses.storeFiles.apply( new GetStoreFileRequest( storeId, file, requiredTxId, databaseName ) );
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
