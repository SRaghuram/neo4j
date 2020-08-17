/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.catchup.v4.databases.GetAllDatabaseIdsResponse;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;
import com.neo4j.causalclustering.messaging.CatchupProtocolMessage;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.storageengine.api.StoreId;

/**
 * This class defines a client which "speaks" the various versions of the Catchup protocol. Basically wraps a builder for {@link CatchupProtocolMessage}s.
 */
public interface VersionedCatchupClients extends AutoCloseable
{
    /**
     * Creates a {@link CatchupRequestBuilder}, though returns as a {@link NeedsResponseHandler} to allow the compiler to enforce build steps.
     * Short circuits the builder to the second step (avoiding some kind of explicit `getBuilder()` call).
     *
     * @param v3Request the operation to be invoked against a CatchupClient in the event that V3 of the protocol is agreed during handshake
     * @param <RESULT> the type of result expected when executing he operation in {@code v1Request}
     * @return the second stage of the {@link CatchupRequestBuilder} step builder.
     */
    <RESULT> NeedsV4Handler<RESULT> v3( Function<CatchupClientV3,PreparedRequest<RESULT>> v3Request );

    /** Step builder interface for Catchup requests (instances of {@link CatchupProtocolMessage}) against multiple versions of the protocol */
    interface CatchupRequestBuilder<RESULT>
            extends NeedsV3Handler<RESULT>, NeedsV4Handler<RESULT>, NeedsResponseHandler<RESULT>, IsPrepared<RESULT>
    {
    }

    /**
     * {@link CatchupRequestBuilder} Step 1
     */
    interface NeedsV3Handler<RESULT>
    {
        NeedsV4Handler<RESULT> v3( Function<CatchupClientV3,PreparedRequest<RESULT>> v3Request );
    }

    /**
     * {@link CatchupRequestBuilder} Step 2
     */
    interface NeedsV4Handler<RESULT>
    {
        NeedsResponseHandler<RESULT> v4( Function<CatchupClientV4,PreparedRequest<RESULT>> v4Request );
    }

    /** {@link CatchupRequestBuilder} Step 3 */
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

    interface CatchupClientV3
    {
        PreparedRequest<NamedDatabaseId> getDatabaseId( String databaseName );

        PreparedRequest<CoreSnapshot> getCoreSnapshot( NamedDatabaseId namedDatabaseId );

        PreparedRequest<StoreId> getStoreId( NamedDatabaseId namedDatabaseId );

        PreparedRequest<TxStreamFinishedResponse> pullTransactions( StoreId storeId, long previousTxId, NamedDatabaseId namedDatabaseId );

        PreparedRequest<PrepareStoreCopyResponse> prepareStoreCopy( StoreId storeId, NamedDatabaseId namedDatabaseId );

        PreparedRequest<StoreCopyFinishedResponse> getStoreFile( StoreId storeId, Path path, long requiredTxId, NamedDatabaseId namedDatabaseId );

        default PreparedRequest<GetAllDatabaseIdsResponse> getAllDatabaseIds()
        {
            throw new UnsupportedOperationException( "Not supported in V3" );
        }
    }

    interface CatchupClientV4 extends CatchupClientV3
    {
        PreparedRequest<GetAllDatabaseIdsResponse> getAllDatabaseIds();
    }

    @FunctionalInterface
    interface PreparedRequest<RESULT>
    {
        CompletableFuture<RESULT> execute( CatchupResponseCallback<RESULT> responseHandler );
    }
}
