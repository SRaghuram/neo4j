/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.identity.StoreId;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;
import com.neo4j.causalclustering.protocol.Protocol;

import java.io.File;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * This class defines a client which "speaks" the various versions of the Catchup protocol. Basically wraps a builder for {@link CatchupProtocolMessage}s.
 */
public interface VersionedCatchupClients extends AutoCloseable
{
    /**
     * Creates a {@link CatchupRequestBuilder}, though returns as a {@link NeedsV2Handler} to allow the compiler to enforce build steps.
     * Short circuits the builder to the second step (avoiding some kind of explicit `getBuilder()` call).
     *
     * @param v1Request the operation to be invoked against a CatchupClient in the event that V1 of the protocol is agreed during handshake
     * @param <RESULT> the type of result expected when executing he operation in `v1Request`
     * @return the second stage of the {@link CatchupRequestBuilder} step builder.
     */
    <RESULT> NeedsV2Handler<RESULT> v1( Function<CatchupClientV1,PreparedRequest<RESULT>> v1Request );

    /**
     * Creates a {@link CatchupRequestBuilder}, though returns as a {@link NeedsResponseHandler} to allow the compiler to enforce build steps.
     * Short circuits the builder to the third step (avoiding some kind of explicit `getBuilder()` call).
     *
     * @param allVersionsRequest the operation to be invoked against a CatchupClient, regardless of protocol version.
     * @param <RESULT> the type of result expected when executing he operation in `allVersionsRequest`
     * @return the third stage of the {@link CatchupRequestBuilder} step builder.
     */
    <RESULT> NeedsResponseHandler<RESULT> any( Function<CatchupClientCommon,PreparedRequest<RESULT>> allVersionsRequest );

    /** Step builder interface for Catchup requests (instances of {@link CatchupProtocolMessage}) against multiple versions of the protocol */
    interface CatchupRequestBuilder<RESULT>
            extends NeedsV1Handler<RESULT>, NeedsV2Handler<RESULT>, NeedsV3Handler<RESULT>, NeedsResponseHandler<RESULT>, IsPrepared<RESULT>
    {
    }

    /** {@link CatchupRequestBuilder} Step 1 */
    interface NeedsV1Handler<RESULT>
    {
        NeedsV2Handler<RESULT> v1( Function<CatchupClientV1,PreparedRequest<RESULT>> v1Request );

        NeedsResponseHandler<RESULT> any( Function<CatchupClientCommon,PreparedRequest<RESULT>> allVersionsRequest );
    }

    /** {@link CatchupRequestBuilder} Step 2 */
    interface NeedsV2Handler<RESULT>
    {
        NeedsV3Handler<RESULT> v2( Function<CatchupClientV2,PreparedRequest<RESULT>> v2Request );

        NeedsResponseHandler<RESULT> any( Function<CatchupClientCommon,PreparedRequest<RESULT>> allVersionsRequest );
    }

    /** {@link CatchupRequestBuilder} Step 3 */
    interface NeedsV3Handler<RESULT>
    {
        NeedsResponseHandler<RESULT> v3( Function<CatchupClientV3,PreparedRequest<RESULT>> v3Request );

        NeedsResponseHandler<RESULT> any( Function<CatchupClientCommon,PreparedRequest<RESULT>> allVersionsRequest );
    }

    /** {@link CatchupRequestBuilder} Step 4 */
    interface NeedsResponseHandler<RESULT>
    {
        IsPrepared<RESULT> withResponseHandler( CatchupResponseCallback<RESULT> responseHandler );
    }

    /** {@link CatchupRequestBuilder} Final step */
    interface IsPrepared<RESULT>
    {
        RESULT request() throws Exception;
    }

    /* Interfaces for CatchupClients (and their helper return type, PreparedRequest) below here. These
     * clients shouldn't really be created directly, instead the should be created by the class implementing
     * CatchupRequestBuilder. Instances of this type should only really be used within lambda's passed to
     * methods of said builder. See CatchupClient for a reference implementation. */

    interface CatchupClientCommon
    {
        PreparedRequest<CoreSnapshot> getCoreSnapshot();
    }

    interface CatchupClientV1 extends CatchupClientCommon
    {
        PreparedRequest<StoreId> getStoreId();

        PreparedRequest<TxStreamFinishedResponse> pullTransactions( StoreId storeId, long previousTxId );

        PreparedRequest<PrepareStoreCopyResponse> prepareStoreCopy( StoreId storeId );

        PreparedRequest<StoreCopyFinishedResponse> getIndexFiles( StoreId storeId, long indexId, long requiredTxId );

        PreparedRequest<StoreCopyFinishedResponse> getStoreFile( StoreId storeId, File file, long requiredTxId );
    }

    interface CatchupClientV2 extends CatchupClientCommon
    {
        PreparedRequest<StoreId> getStoreId( String databaseName );

        PreparedRequest<TxStreamFinishedResponse> pullTransactions( StoreId storeId, long previousTxId, String databaseName );

        PreparedRequest<PrepareStoreCopyResponse> prepareStoreCopy( StoreId storeId, String databaseName );

        PreparedRequest<StoreCopyFinishedResponse> getIndexFiles( StoreId storeId, long indexId, long requiredTxId, String databaseName );

        PreparedRequest<StoreCopyFinishedResponse> getStoreFile( StoreId storeId, File file, long requiredTxId, String databaseName );
    }

    interface CatchupClientV3 extends CatchupClientCommon
    {
        PreparedRequest<StoreId> getStoreId( String databaseName );

        PreparedRequest<TxStreamFinishedResponse> pullTransactions( StoreId storeId, long previousTxId, String databaseName );

        PreparedRequest<PrepareStoreCopyResponse> prepareStoreCopy( StoreId storeId, String databaseName );

        PreparedRequest<StoreCopyFinishedResponse> getIndexFiles( StoreId storeId, long indexId, long requiredTxId, String databaseName );

        PreparedRequest<StoreCopyFinishedResponse> getStoreFile( StoreId storeId, File file, long requiredTxId, String databaseName );
    }

    @FunctionalInterface
    interface PreparedRequest<RESULT>
    {
        CompletableFuture<RESULT> execute( CatchupResponseCallback<RESULT> responseHandler );
    }
}
