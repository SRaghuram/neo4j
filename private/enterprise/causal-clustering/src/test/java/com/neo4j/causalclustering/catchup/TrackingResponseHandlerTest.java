/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.FileChunk;
import org.junit.jupiter.api.Test;

import java.nio.channels.ClosedChannelException;
import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.fail;

class TrackingResponseHandlerTest
{
    @Test
    void shouldResetTimestampWhenNewHandlerIsRegistered()
    {
        CatchupResponseAdaptor catchUpResponseAdaptor = new CatchupResponseAdaptor()
        {
            @Override
            public boolean onFileContent( CompletableFuture signal, FileChunk response )
            {
                return true;
            }
        };

        FakeClock fakeClock = Clocks.fakeClock();

        TrackingResponseHandler trackingResponseHandler = new TrackingResponseHandler( fakeClock, null );
        trackingResponseHandler.setResponseHandler( catchUpResponseAdaptor, new CompletableFuture<>() );

        assertFalse( trackingResponseHandler.millisSinceLastResponse().isPresent() );

        trackingResponseHandler.onFileContent( null );

        assertEquals( 0L, trackingResponseHandler.millisSinceLastResponse().getAsLong() );

        trackingResponseHandler.setResponseHandler( catchUpResponseAdaptor, new CompletableFuture<>() );

        assertFalse( trackingResponseHandler.millisSinceLastResponse().isPresent() );
    }

    @Test
    void newResponseHandlerShouldFailImmediatelyIfChannelIsAlreadyClosed() throws Exception
    {
        // given
        var trackingResponseHandler = new TrackingResponseHandler( Clock.systemDefaultZone(), null );
        var requestOutcomeSignal = new CompletableFuture<Void>();
        trackingResponseHandler.onClose();

        // when
        trackingResponseHandler.setResponseHandler( new CatchupResponseAdaptor<Void>(), requestOutcomeSignal );

        // then
        assertThat( requestOutcomeSignal.isDone() ).isTrue();
        try
        {
            requestOutcomeSignal.get();
            fail();
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause() ).isInstanceOf( ClosedChannelException.class );
        }
    }
}
