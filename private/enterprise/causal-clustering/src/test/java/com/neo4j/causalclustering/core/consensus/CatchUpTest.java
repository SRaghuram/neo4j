/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.consensus.RaftMessages.NewEntry.Request;
import com.neo4j.causalclustering.core.consensus.log.RaftLogHelper;
import com.neo4j.causalclustering.core.consensus.log.ReadableRaftLog;
import com.neo4j.causalclustering.core.consensus.membership.MembershipEntry;
import com.neo4j.causalclustering.core.consensus.shipping.RaftLogShipper;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static com.neo4j.causalclustering.core.consensus.ReplicatedInteger.valueOf;
import static com.neo4j.causalclustering.identity.RaftTestMember.raftMember;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.empty;

class CatchUpTest
{
    @Test
    void happyClusterPropagatesUpdates() throws Throwable
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final RaftMemberId leader = raftMember( 0 );
        final RaftMemberId[] allMembers = {leader, raftMember( 1 ), raftMember( 2 )};

        final RaftTestFixture fixture = new RaftTestFixture( net, 3, allMembers );
        fixture.bootstrap( allMembers );
        final RaftMemberId leaderMember = fixture.members().withId( leader ).member();

        // when
        fixture.members().withId( leader ).timerService().invoke( RaftMachine.Timeouts.ELECTION );
        net.processMessages();
        fixture.members().withId( leader ).raftInstance().handle( new Request( leaderMember, valueOf( 42 ) ) );
        net.processMessages();

        // then
        for ( RaftMemberId aMember : allMembers )
        {
            MatcherAssert.assertThat( fixture.messageLog(), integerValues( fixture.members().withId( aMember ).raftLog() ), hasItems( 42 ) );
        }
    }

    @Test
    void newMemberWithNoLogShouldCatchUpFromPeers() throws Throwable
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final RaftMemberId leaderId = raftMember( 0 );
        final RaftMemberId sleepyId = raftMember( 2 );

        final RaftMemberId[] awakeMembers = {leaderId, raftMember( 1 )};
        final RaftMemberId[] allMembers = {leaderId, raftMember( 1 ), sleepyId};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, allMembers );
        fixture.bootstrap( allMembers );
        fixture.members().withId( leaderId ).raftInstance().installCoreState(
                new RaftCoreState( new MembershipEntry( 0, new HashSet<>( Arrays.asList( allMembers ) ) ) ));

        fixture.members().withId( leaderId ).timerService().invoke( RaftMachine.Timeouts.ELECTION );
        net.processMessages();

        final RaftMemberId leader = fixture.members().withId( leaderId ).member();

        net.disconnect( sleepyId );

        // when
        fixture.members().withId( leaderId ).raftInstance().handle( new Request( leader, valueOf( 10 ) ) );
        fixture.members().withId( leaderId ).raftInstance().handle( new Request( leader, valueOf( 20 ) ) );
        fixture.members().withId( leaderId ).raftInstance().handle( new Request( leader, valueOf( 30 ) ) );
        fixture.members().withId( leaderId ).raftInstance().handle( new Request( leader, valueOf( 40 ) ) );
        net.processMessages();

        // then
        for ( RaftMemberId awakeMember : awakeMembers )
        {
            MatcherAssert.assertThat( integerValues( fixture.members().withId( awakeMember ).raftLog() ),
                    hasItems( 10, 20, 30, 40 ) );
        }

        MatcherAssert.assertThat( integerValues( fixture.members().withId( sleepyId ).raftLog() ), empty() );

        // when
        net.reconnect( sleepyId );
        fixture.members().invokeTimeout( RaftLogShipper.Timeouts.RESEND );
        net.processMessages();

        // then
        MatcherAssert.assertThat( fixture.messageLog(), integerValues( fixture.members().withId( sleepyId ).raftLog() ), hasItems( 10, 20, 30, 40 ) );
    }

    private List<Integer> integerValues( ReadableRaftLog log ) throws IOException
    {
        List<Integer> actual = new ArrayList<>();
        for ( long logIndex = 0; logIndex <= log.appendIndex(); logIndex++ )
        {
            ReplicatedContent content = RaftLogHelper.readLogEntry( log, logIndex ).content();
            if ( content instanceof ReplicatedInteger )
            {
                ReplicatedInteger integer = (ReplicatedInteger) content;
                actual.add( integer.get() );
            }
        }
        return actual;
    }
}
