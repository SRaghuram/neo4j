/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.replication;

import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.time.Duration;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import static java.time.temporal.ChronoUnit.MILLIS;
import static java.time.temporal.ChronoUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@TestInstance( TestInstance.Lifecycle.PER_CLASS )
class LeaderProviderTest
{
    private static final MemberId MEMBER_ID = new MemberId( UUID.randomUUID() );
    private final ExecutorService executorService = Executors.newCachedThreadPool();

    @AfterAll
    void after()
    {
        executorService.shutdown();
    }

    @Test
    void shouldGiveCurrentLeaderImmediatelyIfAvailable() throws InterruptedException
    {
        LeaderProvider leaderProvider = new LeaderProvider( Duration.of( 0, SECONDS ) );
        leaderProvider.setLeader( MEMBER_ID );

        assertEquals( leaderProvider.awaitLeader(), MEMBER_ID );
    }

    @Test
    void shouldTriggerWakeupOfWaitingThreads() throws InterruptedException, ExecutionException, TimeoutException
    {
        // given
        int threads = 3;
        LeaderProvider leaderProvider = new LeaderProvider( Duration.of( 15, SECONDS ) );
        assertNull( leaderProvider.currentLeader() );

        // when
        CompletableFuture<ArrayList<MemberId>> futures = CompletableFuture.completedFuture( new ArrayList<>() );
        for ( int i = 0; i < threads; i++ )
        {
            CompletableFuture<MemberId> future = CompletableFuture.supplyAsync( awaitLeader( leaderProvider ), executorService );
            futures = futures.thenCombine( future, ( completableFutures, memberId ) ->
            {
                completableFutures.add( memberId );
                return completableFutures;
            } );
        }

        // then
        Thread.sleep( 10 );
        assertFalse( futures.isDone() );

        // when
        leaderProvider.setLeader( MEMBER_ID );

        ArrayList<MemberId> memberIds = futures.get( 5, TimeUnit.SECONDS );

        // then
        assertTrue( memberIds.stream().allMatch( memberId -> memberId.equals( MEMBER_ID ) ) );
    }

    @Test
    void shouldTimeoutWaitingForLeaderThatNeverComes() throws InterruptedException
    {
        // given
        LeaderProvider leaderProvider = new LeaderProvider( Duration.of( 10, MILLIS ) );
        assertNull( leaderProvider.currentLeader() );

        // then
        assertNull( leaderProvider.awaitLeader() );
    }

    private Supplier<MemberId> awaitLeader( LeaderProvider leaderProvider )
    {
        return () ->
        {
            try
            {
                return leaderProvider.awaitLeader();
            }
            catch ( InterruptedException e )
            {
                throw new RuntimeException( e );
            }
        };
    }
}
