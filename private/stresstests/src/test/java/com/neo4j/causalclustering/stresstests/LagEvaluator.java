/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.stresstests;

import java.time.Clock;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Supplier;

import static java.lang.StrictMath.floorDiv;
import static java.lang.StrictMath.multiplyExact;
import static java.lang.String.format;

class LagEvaluator
{
    class Lag
    {
        private final long timeLagMillis;
        private final long valueLag;

        Lag( long timeLagMillis, long valueLag )
        {
            this.timeLagMillis = timeLagMillis;
            this.valueLag = valueLag;
        }

        long timeLagMillis()
        {
            return timeLagMillis;
        }

        @Override
        public String toString()
        {
            return "Lag{" + "timeLagMillis=" + timeLagMillis + ", valueLag=" + valueLag + '}';
        }
    }

    private final Supplier<OptionalLong> leader;
    private final Supplier<OptionalLong> follower;
    private final Clock clock;

    private Sample previous = Sample.INCOMPLETE;

    LagEvaluator( Supplier<OptionalLong> leader, Supplier<OptionalLong> follower, Clock clock )
    {
        this.leader = leader;
        this.follower = follower;
        this.clock = clock;
    }

    Optional<Lag> evaluate()
    {
        Sample current = sampleNow();
        Optional<Lag> lag = estimateLag( previous, current );
        previous = current;
        return lag;
    }

    private Optional<Lag> estimateLag( Sample previous, Sample current )
    {
        if ( previous.incomplete() || current.incomplete() )
        {
            return Optional.empty();
        }

        if ( current.timeStampMillis <= previous.timeStampMillis )
        {
            throw new RuntimeException( format( "Time not progressing: %s -> %s", previous, current ) );
        }
        else if ( current.follower < previous.follower )
        {
            throw new RuntimeException( format( "Follower going backwards: %s -> %s", previous, current ) );
        }
        else if ( current.follower == previous.follower )
        {
            return Optional.empty();
        }

        long valueLag = current.leader - current.follower;
        long dTime = current.timeStampMillis - previous.timeStampMillis;
        long dFollower = current.follower - previous.follower;
        long timeLagMillis = floorDiv( multiplyExact( valueLag, dTime ), dFollower );

        return Optional.of( new Lag( timeLagMillis, valueLag ) );
    }

    private Sample sampleNow()
    {
        // sample follower before leader, to avoid erroneously observing the follower as being ahead
        OptionalLong followerSample = follower.get();
        OptionalLong leaderSample = leader.get();

        if ( !followerSample.isPresent() || !leaderSample.isPresent() )
        {
            return Sample.INCOMPLETE;
        }

        return new Sample( leaderSample.getAsLong(), followerSample.getAsLong(), clock.millis() );
    }

    private static class Sample
    {
        private static final Sample INCOMPLETE = new Sample()
        {
            @Override
            boolean incomplete()
            {
                return true;
            }
        };

        private final long timeStampMillis;
        private final long leader;
        private final long follower;

        Sample()
        {
            timeStampMillis = 0;
            follower = 0;
            leader = 0;
        }

        Sample( long leader, long follower, long timeStampMillis )
        {
            this.timeStampMillis = timeStampMillis;
            this.leader = leader;
            this.follower = follower;
        }

        boolean incomplete()
        {
            return false;
        }
    }
}
