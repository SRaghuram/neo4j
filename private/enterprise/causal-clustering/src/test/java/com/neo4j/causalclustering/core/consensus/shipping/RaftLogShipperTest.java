/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.shipping;

import com.neo4j.causalclustering.core.consensus.LeaderContext;
import com.neo4j.causalclustering.core.consensus.OutboundMessageCollector;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.RaftMessages.AppendEntries;
import com.neo4j.causalclustering.core.consensus.ReplicatedInteger;
import com.neo4j.causalclustering.core.consensus.ReplicatedString;
import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.log.RaftLogEntry;
import com.neo4j.causalclustering.core.consensus.log.cache.ConsecutiveInFlightCache;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.test.matchers.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.time.Clocks;

import static com.neo4j.causalclustering.identity.RaftTestMember.raftMember;
import static com.neo4j.test.matchers.Matchers.hasMessage;
import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createScheduler;

@ExtendWith( LifeExtension.class )
class RaftLogShipperTest
{
    @Inject
    private LifeSupport life;

    private JobScheduler scheduler = createScheduler();
    private OutboundMessageCollector outbound;
    private RaftLog raftLog;
    private Clock clock;
    private TimerService timerService;
    private RaftMemberId leader;
    private RaftMemberId follower;
    private long leaderTerm;
    private long leaderCommit;
    private long retryTimeMillis;
    private int catchupBatchSize = 64;
    private int maxAllowedShippingLag = 256;
    private LogProvider logProvider;
    private Log log;

    private RaftLogShipper logShipper;

    private RaftLogEntry entry0 = new RaftLogEntry( 0, ReplicatedInteger.valueOf( 1000 ) );
    private RaftLogEntry entry1 = new RaftLogEntry( 0, ReplicatedString.valueOf( "kedha" ) );
    private RaftLogEntry entry2 = new RaftLogEntry( 0, ReplicatedInteger.valueOf( 2000 ) );
    private RaftLogEntry entry3 = new RaftLogEntry( 0, ReplicatedString.valueOf( "chupchick" ) );

    @BeforeEach
    void setup()
    {
        life.add( scheduler );
        // defaults
        outbound = new OutboundMessageCollector();
        raftLog = new InMemoryRaftLog();
        clock = Clocks.systemClock();
        leader = raftMember( 0 );
        follower = raftMember( 1 );
        leaderTerm = 0;
        leaderCommit = 0;
        retryTimeMillis = 100000;
        logProvider = mock( LogProvider.class );
        timerService = new TimerService( scheduler, logProvider );
        log = mock( Log.class );
        when( logProvider.getLog( RaftLogShipper.class ) ).thenReturn( log );
    }

    @AfterEach
    void teardown()
    {
        if ( logShipper != null )
        {
            logShipper.stop();
            logShipper = null;
        }
    }

    private void startLogShipper()
    {
        logShipper = new RaftLogShipper( outbound, logProvider, raftLog, clock, timerService, leader, follower, leaderTerm, leaderCommit,
                        retryTimeMillis, catchupBatchSize, maxAllowedShippingLag, new ConsecutiveInFlightCache() );
        logShipper.start();
    }

    @Test
    void shouldSendLastEntryOnStart() throws Throwable
    {
        // given
        raftLog.append( entry0 );
        raftLog.append( entry1 );

        // when
        startLogShipper();

        // then
        RaftMessages.AppendEntries.Request expected =
                new RaftMessages.AppendEntries.Request( leader, leaderTerm, 0, entry0.term(), RaftLogEntry.empty,
                        leaderCommit );
        assertThat( outbound.sentTo( follower ), hasItem( expected ) );
    }

    @Test
    void shouldSendPreviousEntryOnMismatch() throws Throwable
    {
        // given
        raftLog.append( entry0 );
        raftLog.append( entry1 );
        raftLog.append( entry2 );
        startLogShipper(); // ships entry2 on start

        // when
        outbound.clear();
        logShipper.onMismatch( 0, new LeaderContext( 0, 0 ) );

        // then: we expect it to ship (empty) entry1 next
        RaftMessages.AppendEntries.Request expected =
                new RaftMessages.AppendEntries.Request( leader, leaderTerm, 0, 0, RaftLogEntry.empty, leaderCommit );
        assertThat( outbound.sentTo( follower ), hasItem( expected ) );
    }

    @Test
    void shouldKeepSendingFirstEntryAfterSeveralMismatches() throws Throwable
    {
        // given
        raftLog.append( entry0 );
        raftLog.append( entry1 );
        startLogShipper();

        logShipper.onMismatch( 0, new LeaderContext( 0, 0 ) );
        logShipper.onMismatch( 0, new LeaderContext( 0, 0 ) );

        // when
        outbound.clear();
        logShipper.onMismatch( 0, new LeaderContext( 0, 0 ) );

        // then
        RaftMessages.AppendEntries.Request expected =
                new RaftMessages.AppendEntries.Request( leader, leaderTerm, 0, 0, RaftLogEntry.empty, leaderCommit );
        assertThat( outbound.sentTo( follower ), hasItem( expected ) );
    }

    @Test
    void shouldSendNextBatchAfterMatch() throws Throwable
    {
        // given
        raftLog.append( entry0 );
        raftLog.append( entry1 );
        raftLog.append( entry2 );
        raftLog.append( entry3 );
        startLogShipper();

        logShipper.onMismatch( 0, new LeaderContext( 0, 0 ) );

        // when
        outbound.clear();
        logShipper.onMatch( 0, new LeaderContext( 0, 0 ) );

        // then
        assertThat( outbound.sentTo( follower ), Matchers.hasRaftLogEntries( asList( entry1, entry2, entry3 ) ) );
    }

    @Test
    void shouldSendNewEntriesAfterMatchingLastEntry() throws Throwable
    {
        // given
        raftLog.append( entry0 );
        startLogShipper();

        logShipper.onMatch( 0, new LeaderContext( 0, 0 ) );

        // when
        outbound.clear();

        raftLog.append( entry1 );
        logShipper.onNewEntries( 0, 0, new RaftLogEntry[]{entry1}, new LeaderContext( 0, 0 ) );
        raftLog.append( entry2 );
        logShipper.onNewEntries( 1, 0, new RaftLogEntry[]{entry2}, new LeaderContext( 0, 0 ) );

        // then
        assertThat( outbound.sentTo( follower ), Matchers.hasRaftLogEntries( asList( entry1, entry2 ) ) );
    }

    @Test
    void shouldNotSendNewEntriesWhenNotMatched() throws Throwable
    {
        // given
        raftLog.append( entry0 );
        startLogShipper();

        // when
        outbound.clear();
        logShipper.onNewEntries( 0, 0, new RaftLogEntry[]{entry1}, new LeaderContext( 0, 0 ) );
        logShipper.onNewEntries( 1, 0, new RaftLogEntry[]{entry2}, new LeaderContext( 0, 0 ) );

        // then
        assertEquals( 0, outbound.sentTo( follower ).size() );
    }

    @Test
    void shouldResendLastSentEntryOnFirstMismatch() throws Throwable
    {
        // given
        raftLog.append( entry0 );
        startLogShipper();
        raftLog.append( entry1 );
        raftLog.append( entry2 );

        logShipper.onMatch( 0, new LeaderContext( 0, 0 ) );
        logShipper.onNewEntries( 0, 0, new RaftLogEntry[]{entry1}, new LeaderContext( 0, 0 ) );
        logShipper.onNewEntries( 1, 0, new RaftLogEntry[]{entry2}, new LeaderContext( 0, 0 ) );

        // when
        outbound.clear();
        logShipper.onMismatch( 1, new LeaderContext( 0, 0 ) );

        // then
        RaftMessages.AppendEntries.Request expected =
                new RaftMessages.AppendEntries.Request( leader, leaderTerm, 1, entry1.term(), RaftLogEntry.empty,
                        leaderCommit );
        assertThat( outbound.sentTo( follower ), hasItem( expected ) );
    }

    @Test
    void shouldSendAllEntriesAndCatchupCompletely() throws Throwable
    {
        // given
        final int ENTRY_COUNT = catchupBatchSize * 10;
        Collection<RaftLogEntry> entries = new ArrayList<>();
        for ( int i = 0; i < ENTRY_COUNT; i++ )
        {
            entries.add( new RaftLogEntry( 0, ReplicatedInteger.valueOf( i ) ) );
        }

        for ( RaftLogEntry entry : entries )
        {
            raftLog.append( entry );
        }

        // then
        startLogShipper();

        // back-tracking stage
        RaftMessages.AppendEntries.Request expected =
                new RaftMessages.AppendEntries.Request( leader, leaderTerm, 0, 0, RaftLogEntry.empty, leaderCommit );
        while ( !outbound.sentTo( follower ).contains( expected ) )
        {
            logShipper.onMismatch( -1, new LeaderContext( 0, 0 ) );
        }

        // catchup stage
        long matchIndex;

        do
        {
            AppendEntries.Request last = (AppendEntries.Request) Iterables.last( outbound.sentTo( follower ) );
            matchIndex = last.prevLogIndex() + last.entries().length;

            outbound.clear();
            logShipper.onMatch( matchIndex, new LeaderContext( 0, 0 ) );
        }
        while ( outbound.sentTo( follower ).size() > 0 );

        assertEquals( ENTRY_COUNT - 1, matchIndex );
    }

    @Test
    void shouldSendMostRecentlyAvailableEntryIfPruningHappened() throws IOException
    {
        //given
        raftLog.append( entry0 );
        raftLog.append( entry1 );
        raftLog.append( entry2 );
        raftLog.append( entry3 );

        startLogShipper();

        //when
        raftLog.prune( 2 );
        outbound.clear();
        logShipper.onMismatch( 0, new LeaderContext( 0, 0 ) );

        //then
        RaftMessages.AppendEntries.Request expected = new RaftMessages.AppendEntries.Request( leader, leaderTerm, 2,
                entry2.term(), RaftLogEntry.empty, leaderCommit );
        assertThat( outbound.sentTo( follower ), hasItem( expected ) );
    }

    @Test
    void shouldSendLogCompactionInfoToFollowerOnMatchIfEntryHasBeenPrunedAway() throws Exception
    {
        //given
        raftLog.append( entry0 );
        raftLog.append( entry1 );
        raftLog.append( entry2 );
        raftLog.append( entry3 );

        startLogShipper();

        //when
        outbound.clear();
        raftLog.prune( 2 );

        logShipper.onMatch( 1, new LeaderContext( 0, 0 ) );

        //then
        assertTrue( outbound.hasAnyEntriesTo( follower ) );
        assertThat( outbound.sentTo( follower ),
                hasMessage( new RaftMessages.LogCompactionInfo( leader, 0, 2 ) ) );
    }

    @Test
    void shouldPickUpAfterMissedBatch() throws Exception
    {
        //given
        raftLog.append( entry0 );
        raftLog.append( entry1 );
        raftLog.append( entry2 );
        raftLog.append( entry3 );

        startLogShipper();
        logShipper.onMatch( 0, new LeaderContext( 0, 0 ) );
        // we are now in PIPELINE mode, because we matched and the entire last batch was sent out

        logShipper.onTimeout();
        // and now we should be in CATCHUP mode, awaiting a late response

        // the response to the batch never came, so on timeout we enter MISMATCH mode and send a single entry based on
        // the latest we knowingly sent (entry3)
        logShipper.onTimeout();
        outbound.clear();

        // fake a match
        logShipper.onMatch( 0, new LeaderContext( 0, 0 ) );

        assertThat( outbound.sentTo( follower ), Matchers.hasRaftLogEntries( asList( entry1, entry2, entry3 ) ) );
    }
}
