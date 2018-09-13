/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import java.nio.channels.ClosedChannelException;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.neo4j.causalclustering.catchup.storecopy.FileChunk;
import org.neo4j.causalclustering.catchup.storecopy.FileHeader;
import org.neo4j.causalclustering.catchup.storecopy.GetStoreIdResponse;
import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import org.neo4j.causalclustering.catchup.tx.TxPullResponse;
import org.neo4j.causalclustering.catchup.tx.TxStreamFinishedResponse;
import org.neo4j.causalclustering.core.state.snapshot.CoreSnapshot;

import static org.neo4j.util.concurrent.Futures.failedFuture;

@SuppressWarnings( "unchecked" )
class TrackingResponseHandler implements CatchupResponseHandler
{
    private static final CompletableFuture<Object> ILLEGAL_FUTURE =
            failedFuture( new IllegalStateException( "Not expected" ) );
    private static final CatchupResponseAdaptor ILLEGAL_HANDLER = new CatchupResponseAdaptor();

    private final Clock clock;

    private CatchupResponseCallback delegate;
    private CompletableFuture<?> requestOutcomeSignal;
    private Long lastResponseTime;

    TrackingResponseHandler( Clock clock )
    {
        this.clock = clock;
        clearResponseHandler();
    }

    void clearResponseHandler()
    {
        this.requestOutcomeSignal = ILLEGAL_FUTURE;
        this.delegate = ILLEGAL_HANDLER;
    }

    void setResponseHandler( CatchupResponseCallback responseHandler, CompletableFuture<?>
            requestOutcomeSignal )
    {
        this.delegate = responseHandler;
        this.requestOutcomeSignal = requestOutcomeSignal;
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
    public void onClose()
    {
        requestOutcomeSignal.completeExceptionally( new ClosedChannelException().fillInStackTrace() );
    }

    Optional<Long> lastResponseTime()
    {
        return Optional.ofNullable( lastResponseTime );
    }

    private void recordLastResponse()
    {
        lastResponseTime = clock.millis();
    }
}
