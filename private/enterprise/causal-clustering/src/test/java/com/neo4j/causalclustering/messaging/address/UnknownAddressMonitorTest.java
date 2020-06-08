/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.address;

import com.neo4j.causalclustering.identity.MemberId;
import org.junit.jupiter.api.Test;

import org.neo4j.logging.Log;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

import static com.neo4j.causalclustering.identity.RaftTestMember.member;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class UnknownAddressMonitorTest
{
    @Test
    void shouldLogFirstFailure()
    {
        // given
        Log log = mock( Log.class );
        UnknownAddressMonitor logger = new UnknownAddressMonitor( log, testClock(), 100 );

        // when
        MemberId to = member( 0 );
        logger.logAttemptToSendToMemberWithNoKnownAddress( to );

        // then
        verify( log ).info( "No address found for %s, probably because the member has been shut down.", to );
    }

    private FakeClock testClock()
    {
        return Clocks.fakeClock( 1_000_000, MILLISECONDS );
    }

    @Test
    void shouldThrottleLogging()
    {
        // given
        Log log = mock( Log.class );
        FakeClock clock = testClock();
        UnknownAddressMonitor logger = new UnknownAddressMonitor( log, clock, 1000 );
        MemberId to = member( 0 );

        // when
        logger.logAttemptToSendToMemberWithNoKnownAddress( to );
        clock.forward( 1, MILLISECONDS );
        logger.logAttemptToSendToMemberWithNoKnownAddress( to );

        // then
        verify( log ).info( "No address found for %s, probably because the member has been shut down.", to );
    }

    @Test
    void shouldResumeLoggingAfterQuietPeriod()
    {
        // given
        Log log = mock( Log.class );
        FakeClock clock = testClock();
        UnknownAddressMonitor logger = new UnknownAddressMonitor( log, clock, 1000 );
        MemberId to = member( 0 );

        // when
        logger.logAttemptToSendToMemberWithNoKnownAddress( to );
        clock.forward( 20001, MILLISECONDS );
        logger.logAttemptToSendToMemberWithNoKnownAddress( to );
        clock.forward( 80001, MILLISECONDS );
        logger.logAttemptToSendToMemberWithNoKnownAddress( to );

        // then
        verify( log, times( 3 ) ).info( "No address found for %s, probably because the member has been shut down.", to );
    }
}
