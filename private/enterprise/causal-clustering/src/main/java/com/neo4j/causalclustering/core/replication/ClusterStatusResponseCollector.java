/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.core.consensus.RaftMessages;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public final class ClusterStatusResponseCollector implements Consumer<RaftMessages.StatusResponse>
{
    private final Map<UUID,CountingResponseWrapper> requestToResponseMap = new ConcurrentHashMap<>();

    @Override
    public void accept( RaftMessages.StatusResponse statusResponse )
    {
        addResponse( statusResponse );
    }

    private void addResponse( RaftMessages.StatusResponse statusResponse )
    {
        requestToResponseMap.computeIfPresent( statusResponse.getRequestId(), ( key, value ) ->
        {
            value.addResponse( statusResponse );
            return value;
        } );
    }

    void expectFollowerStatusesFor( UUID requestId, int numberOfResponses )
    {
        requestToResponseMap.computeIfAbsent( requestId, r -> new CountingResponseWrapper( numberOfResponses ) );
    }

    List<RaftMessages.StatusResponse> getAllStatuses( UUID requestId, Duration timeout ) throws InterruptedException
    {
        var value = requestToResponseMap.get( requestId );
        if ( value == null )
        {
            return List.of();
        }

        value.countdown.await( timeout.toMillis(), TimeUnit.MILLISECONDS );
        requestToResponseMap.remove( requestId );
        return value.responses;
    }

    private static class CountingResponseWrapper
    {
        private final List<RaftMessages.StatusResponse> responses;
        private final CountDownLatch countdown;

        CountingResponseWrapper( int numberOfResponses )
        {
            this.responses = new ArrayList<>();
            this.countdown = new CountDownLatch( numberOfResponses );
        }

        void addResponse( RaftMessages.StatusResponse statusResponse )
        {
            responses.add( statusResponse );
            countdown.countDown();
        }
    }
}
