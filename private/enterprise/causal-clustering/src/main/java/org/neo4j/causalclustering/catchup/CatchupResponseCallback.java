/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.catchup.storecopy.FileChunk;
import org.neo4j.causalclustering.catchup.storecopy.FileHeader;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponse;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import org.neo4j.causalclustering.catchup.tx.TxPullResponse;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

public interface CatchupResponseCallback<T>
{
    void onFileHeader( CompletableFuture<T> signal, FileHeader fileHeader );

    boolean onFileContent( CompletableFuture<T> signal, FileChunk fileChunk );

    void onFileStreamingComplete( CompletableFuture<T> signal, StoreCopyFinishedResponse response );

    void onTxPullResponse( CompletableFuture<T> signal, TxPullResponse tx );

    void onTxStreamFinishedResponse( CompletableFuture<T> signal, TxStreamFinishedResponse response );

    void onGetStoreIdResponse( CompletableFuture<T> signal, GetStoreIdResponse response );

    void onCoreSnapshot( CompletableFuture<T> signal, CoreSnapshot coreSnapshot );

    void onStoreListingResponse( CompletableFuture<T> signal, PrepareStoreCopyResponse prepareStoreCopyResponse );
}
