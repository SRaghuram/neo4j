/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.election;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.RaftMachineBuilder;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeoutException;

import org.neo4j.internal.helpers.collection.FilteringIterable;

import static java.util.stream.Collectors.toList;

/**
 * In this scenario we disconnect the current leader and measure how long time it
 * takes until the remaining members agree on a new leader.
 */
public class DisconnectLeaderScenario
{
    private final Fixture fixture;
    private final long electionTimeout;
    private final List<Long> electionTimeResults = new ArrayList<>();
    private long timeoutCount;

    public DisconnectLeaderScenario( Fixture fixture, long electionTimeout )
    {
        this.fixture = fixture;
        this.electionTimeout = electionTimeout;
    }

    public void run( long iterations, long leaderStabilityMaxTimeMillis ) throws InterruptedException
    {
        for ( int i = 0; i < iterations; i++ )
        {
            long electionTime;
            try
            {
                electionTime = oneIteration( leaderStabilityMaxTimeMillis );
                electionTimeResults.add( electionTime );
            }
            catch ( TimeoutException e )
            {
                timeoutCount++;
            }
            fixture.net.reset();
            Thread.sleep( ThreadLocalRandom.current().nextLong( electionTimeout ) );
        }
    }

    private long oneIteration( long leaderStabilityMaxTimeMillis ) throws InterruptedException, TimeoutException
    {
        List<RaftMachine> rafts = fixture.rafts.stream().map( RaftMachineBuilder.RaftFixture::raftMachine ).collect( toList() );
        RaftMemberId oldLeader = ElectionUtil.waitForLeaderAgreement( rafts, leaderStabilityMaxTimeMillis );
        long startTime = System.currentTimeMillis();

        fixture.net.disconnect( oldLeader );
        RaftMemberId newLeader = ElectionUtil.waitForLeaderAgreement(
                new FilteringIterable<>( rafts, raft -> !raft.memberId().equals( oldLeader ) ),
                leaderStabilityMaxTimeMillis );
        assert !newLeader.equals( oldLeader ); // this should be guaranteed by the waitForLeaderAgreement call

        return System.currentTimeMillis() - startTime;
    }

    private boolean hadOneOrMoreCollisions( long result )
    {
        /* This is just a simple heuristic to classify the results into colliding and
         * non-colliding groups. It is not entirely accurate and doesn't have to be. */
        return result > (electionTimeout * 2);
    }

    public class Result
    {
        double nonCollidingAverage;
        double collidingAverage;
        double collisionRate;
        long collisionCount;
        long timeoutCount;

        @Override
        public String toString()
        {
            return String.format( "Result{nonCollidingAverage=%s, collidingAverage=%s, collisionRate=%s, " +
                            "collisionCount=%d, timeoutCount=%d}",
                    nonCollidingAverage, collidingAverage, collisionRate, collisionCount, timeoutCount );
        }
    }

    public Result result()
    {
        Result result = new Result();

        long collidingRuns = 0;
        long collidingSum = 0;

        long nonCollidingRuns = 0;
        long nonCollidingSum = 0;

        for ( long electionTime : electionTimeResults )
        {
            if ( hadOneOrMoreCollisions( electionTime ) )
            {
                collidingRuns++;
                collidingSum += electionTime;
            }
            else
            {
                nonCollidingRuns++;
                nonCollidingSum += electionTime;
            }
        }

        result.collidingAverage = collidingSum / (double) collidingRuns;
        result.nonCollidingAverage = nonCollidingSum / (double) nonCollidingRuns;
        result.collisionRate = collidingRuns / (double) electionTimeResults.size();
        result.collisionCount = collidingRuns;
        result.timeoutCount = timeoutCount;

        return result;
    }
}
