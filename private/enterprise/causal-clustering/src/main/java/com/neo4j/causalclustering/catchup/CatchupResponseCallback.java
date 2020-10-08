/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.FileChunk;
import com.neo4j.causalclustering.catchup.storecopy.FileHeader;
import com.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponse;
import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import com.neo4j.causalclustering.catchup.tx.TxPullResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import com.neo4j.causalclustering.catchup.v3.databaseid.GetDatabaseIdResponse;
import com.neo4j.causalclustering.catchup.v4.databases.GetAllDatabaseIdsResponse;
import com.neo4j.causalclustering.catchup.v4.info.InfoResponse;
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

import java.util.concurrent.CompletableFuture;

public interface CatchupResponseCallback<T>
{
    void onGetDatabaseIdResponse( CompletableFuture<T> signal, GetDatabaseIdResponse response );

    void onFileHeader( CompletableFuture<T> signal, FileHeader fileHeader );

    boolean onFileContent( CompletableFuture<T> signal, FileChunk fileChunk );

    void onFileStreamingComplete( CompletableFuture<T> signal, StoreCopyFinishedResponse response );

    void onTxPullResponse( CompletableFuture<T> signal, TxPullResponse tx );

    void onTxStreamFinishedResponse( CompletableFuture<T> signal, TxStreamFinishedResponse response );

    void onGetStoreIdResponse( CompletableFuture<T> signal, GetStoreIdResponse response );

    void onCoreSnapshot( CompletableFuture<T> signal, CoreSnapshot coreSnapshot );

    void onStoreListingResponse( CompletableFuture<T> signal, PrepareStoreCopyResponse prepareStoreCopyResponse );

    void onCatchupErrorResponse( CompletableFuture<T> signal, CatchupErrorResponse catchupErrorResponse );

    void onGetAllDatabaseIdsResponse( CompletableFuture<T> signal, GetAllDatabaseIdsResponse response );

    void onInfo( CompletableFuture<T> requestOutcomeSignal, InfoResponse response );
}
