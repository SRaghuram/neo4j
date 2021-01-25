/*
 * Copyright (c) "Neo4j"
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
import com.neo4j.causalclustering.catchup.v4.databases.GetAllDatabaseIdsResponse;
import com.neo4j.causalclustering.catchup.v4.info.InfoResponse;
import com.neo4j.causalclustering.catchup.v4.metadata.GetMetadataResponse;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.helper.OperationProgressMonitor;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;

import java.nio.file.Path;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.NullLog;
import org.neo4j.storageengine.api.StoreId;

import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.failedFuture;

public class MockCatchupClient implements VersionedCatchupClients
{
    private ApplicationProtocol protocol;
    private CatchupClientV3 v3Client;
    private CatchupClientV4 v4Client;
    private CatchupClientV5 v5Client;

    public MockCatchupClient( ApplicationProtocol protocol, CatchupClientV3 v3Client )
    {
        this.protocol = protocol;
        this.v3Client = v3Client;
    }

    public MockCatchupClient( ApplicationProtocol protocol, CatchupClientV4 v4Client )
    {
        this.protocol = protocol;
        this.v4Client = v4Client;
    }

    public MockCatchupClient( ApplicationProtocol protocol, CatchupClientV5 v5Client )
    {
        this.protocol = protocol;
        this.v5Client = v5Client;
    }

    public static MockClientResponses responses()
    {
        return new MockClientResponses();
    }

    @Override
    public <RESULT> NeedsV4Handler<RESULT> v3( Function<CatchupClientV3,PreparedRequest<RESULT>> v3Request )
    {

        Builder<RESULT> reqBuilder = new Builder<>( v3Client, v4Client, v5Client );
        return reqBuilder.v3( v3Request );
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
        private Function<CatchupClientV3,PreparedRequest<RESULT>> v3Request;
        private Function<CatchupClientV4,PreparedRequest<RESULT>> v4Request;
        private Function<CatchupClientV5,PreparedRequest<RESULT>> v5Request;
        private final CatchupClientV3 v3Client;
        private final CatchupClientV4 v4Client;
        private final CatchupClientV5 v5Client;
        private Log log = NullLog.getInstance();

        Builder( CatchupClientV3 v3Client, CatchupClientV4 v4Client, CatchupClientV5 v5Client )
        {
            this.v3Client = v3Client;
            this.v4Client = v4Client;
            this.v5Client = v5Client;
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
        public IsPrepared<RESULT> withResponseHandler( CatchupResponseCallback<RESULT> responseHandler )
        {
            //no-op in the mock for now.
            return this;
        }

        @Override
        public RESULT request() throws Exception
        {
            if ( protocol.equals( ApplicationProtocols.CATCHUP_3_0 ) )
            {
                if ( v3Client == null || v3Request == null )
                {
                    throw new IllegalStateException( "v3Client is not initialized correctly" );
                }
                return withProgressMonitor( v3Request.apply( v3Client ).execute( null ) ).get();
            }
            else if ( protocol.equals( ApplicationProtocols.CATCHUP_4_0 ) )
            {
                if ( v4Client == null || v4Request == null )
                {
                    throw new IllegalStateException( "v4Client is not initialized" );
                }
                return withProgressMonitor( v4Request.apply( v4Client ).execute( null ) ).get();
            }
            else if ( protocol.equals( ApplicationProtocols.CATCHUP_5_0 ) )
            {
                if ( v5Client == null || v5Request == null )
                {
                    throw new IllegalStateException( "v5Client is not initialized" );
                }
                return withProgressMonitor( v5Request.apply( v5Client ).execute( null ) ).get();
            }
            return withProgressMonitor( failedFuture( new Exception( "Unrecognised protocol" ) ) ).get();
        }

        private OperationProgressMonitor<RESULT> withProgressMonitor( CompletableFuture<RESULT> request )
        {
            return OperationProgressMonitor.of( request, 1, () -> OptionalLong.of( 0L ), log );
        }
    }

    public static class MockClientV3 implements CatchupClientV3
    {
        protected final MockClientResponses responses;
        private final DatabaseIdRepository databaseIdRepository;

        public MockClientV3( MockClientResponses responses, DatabaseIdRepository databaseIdRepository )
        {
            this.responses = responses;
            this.databaseIdRepository = databaseIdRepository;
        }

        @Override
        public PreparedRequest<NamedDatabaseId> getDatabaseId( String databaseName )
        {
            return handler -> completedFuture( databaseIdRepository.getByName( databaseName ).get() );
        }

        @Override
        public PreparedRequest<CoreSnapshot> getCoreSnapshot( NamedDatabaseId namedDatabaseId )
        {
            return handler -> completedFuture( responses.coreSnapshot.get() );
        }

        @Override
        public PreparedRequest<StoreId> getStoreId( NamedDatabaseId namedDatabaseId )
        {
            StoreId storeId = responses.storeId.apply( new GetStoreIdRequest( namedDatabaseId.databaseId() ) );
            return handler -> completedFuture( storeId );
        }

        @Override
        public PreparedRequest<TxStreamFinishedResponse> pullTransactions( StoreId storeId, long previousTxId, NamedDatabaseId namedDatabaseId )
        {
            TxStreamFinishedResponse pullResponse =
                    responses.txPullResponse.apply( new TxPullRequest( previousTxId, storeId, namedDatabaseId.databaseId() ) );
            return handler -> completedFuture( pullResponse );
        }

        @Override
        public PreparedRequest<PrepareStoreCopyResponse> prepareStoreCopy( StoreId storeId, NamedDatabaseId namedDatabaseId )
        {
            PrepareStoreCopyResponse prepareStoreCopyResponse =
                    responses.prepareStoreCopyResponse.apply( new PrepareStoreCopyRequest( storeId, namedDatabaseId.databaseId() ) );
            return handler -> completedFuture( prepareStoreCopyResponse );
        }

        @Override
        public PreparedRequest<StoreCopyFinishedResponse> getStoreFile( StoreId storeId, Path file, long requiredTxId, NamedDatabaseId namedDatabaseId )
        {
            StoreCopyFinishedResponse storeCopyFinishedResponse =
                    responses.storeFiles.apply( new GetStoreFileRequest( storeId, file, requiredTxId, namedDatabaseId.databaseId() ) );
            return handler -> completedFuture( storeCopyFinishedResponse );
        }
    }

    public static class MockClientV4 extends MockClientV3 implements CatchupClientV4
    {

        public MockClientV4( MockClientResponses responses, DatabaseIdRepository databaseIdRepository )
        {
            super( responses, databaseIdRepository );
        }

        @Override
        public PreparedRequest<GetAllDatabaseIdsResponse> getAllDatabaseIds()
        {
            return handler -> completedFuture( responses.allDatabaseIdsResponse );
        }

        @Override
        public PreparedRequest<InfoResponse> getReconciledInfo( NamedDatabaseId databaseId )
        {
            return handler -> completedFuture( responses.reconciledInfoResponse );
        }

        @Override
        public PreparedRequest<GetMetadataResponse> getMetadata( String databaseName, String includeMetadata )
        {
            throw new IllegalStateException( "Method is not implemented" );
        }
    }

    public static class MockClientV5 extends MockClientV4 implements CatchupClientV5
    {

        public MockClientV5( MockClientResponses responses, DatabaseIdRepository databaseIdRepository )
        {
            super( responses, databaseIdRepository );
        }
    }

    public static class MockClientResponses
    {

        private Supplier<CoreSnapshot> coreSnapshot;
        private Function<GetStoreIdRequest,StoreId> storeId;
        private Function<TxPullRequest,TxStreamFinishedResponse> txPullResponse;
        private Function<PrepareStoreCopyRequest,PrepareStoreCopyResponse> prepareStoreCopyResponse;
        private Function<GetStoreFileRequest,StoreCopyFinishedResponse> storeFiles;
        private GetAllDatabaseIdsResponse allDatabaseIdsResponse;
        private InfoResponse reconciledInfoResponse;

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

        public MockClientResponses withAllDatabaseResponse( GetAllDatabaseIdsResponse allDatabaseIdsResponse )
        {
            this.allDatabaseIdsResponse = allDatabaseIdsResponse;
            return this;
        }

        public MockClientResponses withReconciledTxId( InfoResponse reconciledInfoResponse )
        {
            this.reconciledInfoResponse = reconciledInfoResponse;
            return this;
        }
    }
}
