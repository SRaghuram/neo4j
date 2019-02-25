/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.FileChunk;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static org.junit.jupiter.api.Assertions.assertEquals;

class TrackingResponseHandlerTest
{
    @Test
    void shouldResetTimeStampWhenNewHandlerIsRegisterd()
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

        TrackingResponseHandler trackingResponseHandler = new TrackingResponseHandler( fakeClock );
        trackingResponseHandler.setResponseHandler( catchUpResponseAdaptor, new CompletableFuture<>() );

        assertEquals( Optional.empty(), trackingResponseHandler.lastResponseTime() );

        trackingResponseHandler.onFileContent( null );

        assertEquals( 0L, trackingResponseHandler.lastResponseTime().get() );

        trackingResponseHandler.setResponseHandler( catchUpResponseAdaptor, new CompletableFuture<>() );

        assertEquals( Optional.empty(), trackingResponseHandler.lastResponseTime() );
    }
}
