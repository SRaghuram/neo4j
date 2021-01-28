/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.FileChunk;
import com.neo4j.causalclustering.catchup.storecopy.FileHeader;
import com.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponse;
import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import com.neo4j.causalclustering.catchup.tx.ReceivedTxPullResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdResponse;
import com.neo4j.causalclustering.catchup.v4.databases.GetAllDatabaseIdsResponse;
import com.neo4j.causalclustering.catchup.v4.info.InfoResponse;
import com.neo4j.causalclustering.catchup.v4.metadata.GetMetadataResponse;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

import java.util.concurrent.CompletableFuture;

import static java.lang.String.format;

public class CatchupResponseAdaptor<T> implements CatchupResponseCallback<T>
{
    @Override
    public void onGetDatabaseIdResponse( CompletableFuture<T> signal, GetDatabaseIdResponse response )
    {
        unimplementedMethod( signal, response );
    }

    @Override
    public void onFileHeader( CompletableFuture<T> signal, FileHeader response )
    {
        unimplementedMethod( signal, response );
    }

    @Override
    public boolean onFileContent( CompletableFuture<T> signal, FileChunk response )
    {
        unimplementedMethod( signal, response );
        return false;
    }

    @Override
    public void onFileStreamingComplete( CompletableFuture<T> signal, StoreCopyFinishedResponse response )
    {
        unimplementedMethod( signal, response );
    }

    @Override
    public void onTxPullResponse( CompletableFuture<T> signal, ReceivedTxPullResponse response )
    {
        unimplementedMethod( signal, response );
    }

    @Override
    public void onTxStreamFinishedResponse( CompletableFuture<T> signal, TxStreamFinishedResponse response )
    {
        unimplementedMethod( signal, response );
    }

    @Override
    public void onGetStoreIdResponse( CompletableFuture<T> signal, GetStoreIdResponse response )
    {
        unimplementedMethod( signal, response );
    }

    @Override
    public void onCoreSnapshot( CompletableFuture<T> signal, CoreSnapshot response )
    {
        unimplementedMethod( signal, response );
    }

    @Override
    public void onStoreListingResponse( CompletableFuture<T> signal, PrepareStoreCopyResponse response )
    {
        unimplementedMethod( signal, response );
    }

    @Override
    public void onGetAllDatabaseIdsResponse( CompletableFuture<T> signal, GetAllDatabaseIdsResponse response )
    {
        unimplementedMethod( signal, response );
    }

    @Override
    public void onInfo( CompletableFuture<T> signal, InfoResponse response )
    {
        unimplementedMethod( signal, response );
    }

    @Override
    public void onGetMetadataResponse( CompletableFuture<T> signal, GetMetadataResponse response )
    {
        unimplementedMethod( signal, response );
    }

    @Override
    public void onCatchupErrorResponse( CompletableFuture<T> signal, CatchupErrorResponse catchupErrorResponse )
    {
        signal.completeExceptionally( new RuntimeException(
                format( "Request returned an error [Status: '%s' Message: '%s']", catchupErrorResponse.status(), catchupErrorResponse.message() ) ) );
    }

    private <U> void unimplementedMethod( CompletableFuture<T> signal, U response )
    {
        signal.completeExceptionally( new CatchUpProtocolViolationException( "This Adaptor has unimplemented methods for: %s", response ) );
    }
}
