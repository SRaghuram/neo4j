/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
import com.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

import java.nio.channels.ClosedChannelException;
import java.time.Clock;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static org.neo4j.util.concurrent.Futures.failedFuture;

@SuppressWarnings( "unchecked" )
class TrackingResponseHandler implements CatchupResponseHandler
{
    private static final CompletableFuture<Object> ILLEGAL_FUTURE =
            failedFuture( new IllegalStateException( "Not expected" ) );
    private static final CatchupResponseAdaptor ILLEGAL_HANDLER = new CatchupResponseAdaptor();
    private static final long NO_RESPONSE_TIME = 1;

    private final Clock clock;

    private CatchupResponseCallback delegate;
    private CompletableFuture<?> requestOutcomeSignal;
    private long lastResponseTime = NO_RESPONSE_TIME;

    TrackingResponseHandler( Clock clock )
    {
        this.clock = clock;
        clearResponseHandler();
    }

    void clearResponseHandler()
    {
        this.requestOutcomeSignal = ILLEGAL_FUTURE;
        this.delegate = ILLEGAL_HANDLER;
        this.lastResponseTime = NO_RESPONSE_TIME;
    }

    void setResponseHandler( CatchupResponseCallback responseHandler, CompletableFuture<?>
            requestOutcomeSignal )
    {
        this.delegate = responseHandler;
        this.requestOutcomeSignal = requestOutcomeSignal;
        this.lastResponseTime = NO_RESPONSE_TIME;
    }

    @Override
    public void onFileHeader( FileHeader fileHeader )
    {
        if ( !requestOutcomeSignal.isCancelled() )
        {
            recordLastResponse();
            delegate.onFileHeader( requestOutcomeSignal, fileHeader );
        }
    }

    @Override
    public boolean onFileContent( FileChunk fileChunk )
    {
        if ( !requestOutcomeSignal.isCancelled() )
        {
            recordLastResponse();
            return delegate.onFileContent( requestOutcomeSignal, fileChunk );
        }
        // true means stop
        return true;
    }

    @Override
    public void onFileStreamingComplete( StoreCopyFinishedResponse response )
    {
        if ( !requestOutcomeSignal.isCancelled() )
        {
            recordLastResponse();
            delegate.onFileStreamingComplete( requestOutcomeSignal, response );
        }
    }

    @Override
    public void onTxPullResponse( TxPullResponse tx )
    {
        if ( !requestOutcomeSignal.isCancelled() )
        {
            recordLastResponse();
            delegate.onTxPullResponse( requestOutcomeSignal, tx );
        }
    }

    @Override
    public void onTxStreamFinishedResponse( TxStreamFinishedResponse response )
    {
        if ( !requestOutcomeSignal.isCancelled() )
        {
            recordLastResponse();
            delegate.onTxStreamFinishedResponse( requestOutcomeSignal, response );
        }
    }

    @Override
    public void onGetStoreIdResponse( GetStoreIdResponse response )
    {
        if ( !requestOutcomeSignal.isCancelled() )
        {
            recordLastResponse();
            delegate.onGetStoreIdResponse( requestOutcomeSignal, response );
        }
    }

    @Override
    public void onCoreSnapshot( CoreSnapshot coreSnapshot )
    {
        if ( !requestOutcomeSignal.isCancelled() )
        {
            recordLastResponse();
            delegate.onCoreSnapshot( requestOutcomeSignal, coreSnapshot );
        }
    }

    @Override
    public void onStoreListingResponse( PrepareStoreCopyResponse storeListingRequest )
    {
        if ( !requestOutcomeSignal.isCancelled() )
        {
            recordLastResponse();
            delegate.onStoreListingResponse( requestOutcomeSignal, storeListingRequest );
        }
    }

    @Override
    public void onCatchupErrorResponse( CatchupErrorResponse catchupErrorResponse )
    {
        if ( !requestOutcomeSignal.isCancelled() )
        {
            recordLastResponse();
            delegate.onCatchupErrorResponse( requestOutcomeSignal, catchupErrorResponse );
        }
    }

    @Override
    public void onClose()
    {
        requestOutcomeSignal.completeExceptionally( new ClosedChannelException().fillInStackTrace() );
    }

    OptionalLong millisSinceLastResponse()
    {
        return lastResponseTime == NO_RESPONSE_TIME ? OptionalLong.empty() : OptionalLong.of( clock.millis() - lastResponseTime );
    }

    private void recordLastResponse()
    {
        lastResponseTime = clock.millis();
    }
}
