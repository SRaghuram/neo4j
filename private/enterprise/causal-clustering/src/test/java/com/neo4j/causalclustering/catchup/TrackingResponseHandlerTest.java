/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.FileChunk;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;

import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

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
}
