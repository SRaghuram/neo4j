/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupErrorResponse;
import com.neo4j.causalclustering.catchup.CatchupResponseAdaptor;
import com.neo4j.causalclustering.catchup.CatchupResult;
import com.neo4j.causalclustering.catchup.tx.TxPullResponse;
import com.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;

import java.util.concurrent.CompletableFuture;

import org.neo4j.logging.Log;

class TxPullResponseHandler extends CatchupResponseAdaptor<TxStreamFinishedResponse>
{
    private final CatchupPollingProcess catchupProcess;
    private final Log log;

    TxPullResponseHandler( CatchupPollingProcess catchupProcess, Log log )
    {
        this.catchupProcess = catchupProcess;
        this.log = log;
    }

    @Override
    public void onTxPullResponse( CompletableFuture<TxStreamFinishedResponse> signal, TxPullResponse response )
    {
        catchupProcess.handleTransaction( response.tx() );
        if ( catchupProcess.isCancelled() )
        {
            signal.complete( new TxStreamFinishedResponse( CatchupResult.SUCCESS_END_OF_STREAM, -1 ) );
        }
    }

    @Override
    public void onTxStreamFinishedResponse( CompletableFuture<TxStreamFinishedResponse> signal, TxStreamFinishedResponse response )
    {
        catchupProcess.streamComplete();
        signal.complete( response );
    }

    @Override
    public void onCatchupErrorResponse( CompletableFuture<TxStreamFinishedResponse> signal, CatchupErrorResponse catchupErrorResponse )
    {
        signal.complete( new TxStreamFinishedResponse( catchupErrorResponse.status(), -1 ) );
        log.warn( catchupErrorResponse.message() );
    }

}
