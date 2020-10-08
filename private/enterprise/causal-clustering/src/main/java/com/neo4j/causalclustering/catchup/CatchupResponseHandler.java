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

import java.io.IOException;

public interface CatchupResponseHandler
{
    void onFileHeader( FileHeader fileHeader );

    /**
     * @param fileChunk Part of a file.
     * @return <code>true</code> if this is the last part of the file that is currently being transferred.
     */
    boolean onFileContent( FileChunk fileChunk ) throws IOException;

    void onFileStreamingComplete( StoreCopyFinishedResponse response );

    void onTxPullResponse( TxPullResponse tx );

    void onTxStreamFinishedResponse( TxStreamFinishedResponse response );

    void onGetStoreIdResponse( GetStoreIdResponse response );

    void onGetDatabaseIdResponse( GetDatabaseIdResponse response );

    void onCoreSnapshot( CoreSnapshot coreSnapshot );

    void onStoreListingResponse( PrepareStoreCopyResponse storeListingRequest );

    void onCatchupErrorResponse( CatchupErrorResponse catchupErrorResponse );

    void onGetAllDatabaseIdsResponse( GetAllDatabaseIdsResponse response );

    void onInfo( InfoResponse msg );

    void onClose();
}
