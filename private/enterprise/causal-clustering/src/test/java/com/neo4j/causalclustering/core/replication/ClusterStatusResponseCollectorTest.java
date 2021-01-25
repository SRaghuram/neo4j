/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.state.machines.status.Status;
import com.neo4j.causalclustering.identity.IdFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;

import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static org.assertj.core.api.Assertions.assertThat;

class ClusterStatusResponseCollectorTest
{
    ExecutorService executor;

    @BeforeEach
    void setup()
    {
        executor = newSingleThreadExecutor();
    }

    @AfterEach
    void clean()
    {
        executor.shutdown();
    }

    @Test
    void shouldNotAcceptResponseWithUnknownMessageId() throws InterruptedException
    {
        //given
        var collector = new ClusterStatusResponseCollector();
        var requestId = UUID.randomUUID();
        var statusResponse = new RaftMessages.StatusResponse( IdFactory.randomRaftMemberId(),
                                                              new Status( Status.Message.OK ),
                                                              UUID.randomUUID() );

        //when
        collector.expectFollowerStatusesFor( requestId, 2 );
        executor.submit( () -> collector.accept( statusResponse ) );

        var resultResponses = collector.getAllStatuses( requestId, Duration.ofSeconds( 1 ) );

        //then
        assertThat( resultResponses ).isEmpty();
    }

    @Test
    void shouldGetClusterStatusSuccessfully() throws InterruptedException
    {
        //given
        var collector = new ClusterStatusResponseCollector();
        var requestId = UUID.randomUUID();
        var statusResponse = new RaftMessages.StatusResponse( IdFactory.randomRaftMemberId(),
                                                              new Status( Status.Message.OK ),
                                                              requestId );
        //when
        collector.expectFollowerStatusesFor( requestId, 1 );
        executor.submit( () -> collector.accept( statusResponse ) );

        var resultResponses = collector.getAllStatuses( requestId, Duration.ofSeconds( 1 ) );

        //then
        assertThat( resultResponses ).size().isEqualTo( 1 );
        assertThat( resultResponses ).contains( statusResponse );
    }

    @Test
    void getAllStatusesShouldReturnOnlyResponsesThatWereAddInWaitInterval() throws InterruptedException
    {
        //given
        var collector = new ClusterStatusResponseCollector();
        var requestId = UUID.randomUUID();
        var statusResponse1 = new RaftMessages.StatusResponse( IdFactory.randomRaftMemberId(),
                                                               new Status( Status.Message.OK ),
                                                               requestId );
        var statusResponse2 = new RaftMessages.StatusResponse( IdFactory.randomRaftMemberId(),
                                                               new Status( Status.Message.OK ),
                                                               requestId );
        var responsesList = List.of( statusResponse1, statusResponse2 );
        //when
        collector.expectFollowerStatusesFor( requestId, 1 );
        //submit response with 3s interval
        executor.submit( () -> responsesList.forEach( r ->
                                                      {
                                                          collector.accept( r );
                                                          sleep( 3000 );
                                                      } ) );

        var responses = collector.getAllStatuses( requestId, Duration.ofSeconds( 1 ) );

        //then
        assertThat( responses ).size().isEqualTo( 1 );
        assertThat( responses ).contains( responsesList.get( 0 ) );
    }

    @Test
    void getAllStatusShouldReturnEmptyResultIfThereAreNoSubmittedResponsesInWaitInterval() throws InterruptedException
    {
        //given
        var collector = new ClusterStatusResponseCollector();
        var requestId = UUID.randomUUID();
        var statusResponse1 = new RaftMessages.StatusResponse( IdFactory.randomRaftMemberId(),
                                                               new Status( Status.Message.OK ),
                                                               requestId );
        var statusResponse2 = new RaftMessages.StatusResponse( IdFactory.randomRaftMemberId(),
                                                               new Status( Status.Message.OK ),
                                                               requestId );
        var responsesList = List.of( statusResponse1, statusResponse2 );
        //when
        collector.expectFollowerStatusesFor( requestId, 1 );
        //submit response after 4s
        executor.submit( () -> responsesList.forEach( r ->
                                                      {
                                                          sleep( 3000 );
                                                          collector.accept( r );
                                                      } ) );

        var responses = collector.getAllStatuses( requestId, Duration.ofSeconds( 1 ) );

        //then
        assertThat( responses ).isEmpty();
    }

    private void sleep( long millis )
    {
        try
        {
            Thread.sleep( millis );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }
}
