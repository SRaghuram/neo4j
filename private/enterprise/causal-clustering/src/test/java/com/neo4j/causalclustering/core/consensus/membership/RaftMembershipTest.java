/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.core.consensus.DirectNetworking;
import com.neo4j.causalclustering.core.consensus.RaftTestFixture;
import com.neo4j.causalclustering.identity.RaftMemberId;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.MatcherAssert;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import static com.neo4j.causalclustering.core.consensus.RaftMachine.Timeouts.ELECTION;
import static com.neo4j.causalclustering.core.consensus.RaftMachine.Timeouts.HEARTBEAT;
import static com.neo4j.causalclustering.core.consensus.roles.Role.FOLLOWER;
import static com.neo4j.causalclustering.core.consensus.roles.Role.LEADER;
import static com.neo4j.causalclustering.identity.RaftTestMember.raftMember;

class RaftMembershipTest
{
    @Test
    void shouldNotFormGroupWithoutAnyBootstrapping()
    {
        // given
        DirectNetworking net = new DirectNetworking();

        final RaftMemberId[] ids = {raftMember( 0 ), raftMember( 1 ), raftMember( 2 )};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, ids );

        fixture.members().setTargetMembershipSet( new RaftTestMembers( ids ).getMembers() );
        fixture.members().invokeTimeout( ELECTION );

        // when
        net.processMessages();

        // then
        MatcherAssert.assertThat( fixture.members(), hasCurrentMembers( new RaftTestMembers( new int[0] ) ) );
        Assertions.assertEquals( 0, fixture.members().withRole( LEADER ).size(), fixture.messageLog() );
        Assertions.assertEquals( 3, fixture.members().withRole( FOLLOWER ).size(), fixture.messageLog() );
    }

    @Test
    void shouldAddSingleInstanceToExistingRaftGroup() throws Exception
    {
        // given
        DirectNetworking net = new DirectNetworking();

        final RaftMemberId leader = raftMember( 0 );
        final RaftMemberId stable1 = raftMember( 1 );
        final RaftMemberId stable2 = raftMember( 2 );
        final RaftMemberId toBeAdded = raftMember( 3 );

        final RaftMemberId[] initialMembers = {leader, stable1, stable2};
        final RaftMemberId[] finalMembers = {leader, stable1, stable2, toBeAdded};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, finalMembers );
        fixture.bootstrap( initialMembers );

        fixture.members().withId( leader ).timerService().invoke( ELECTION );
        net.processMessages();

        // when
        fixture.members().withId( leader ).raftInstance()
                .setTargetMembershipSet( new RaftTestMembers( finalMembers ).getMembers() );
        net.processMessages();

        fixture.members().withId( leader ).timerService().invoke( HEARTBEAT );
        net.processMessages();

        // then
        MatcherAssert.assertThat( fixture.members().withIds( finalMembers ), hasCurrentMembers( new RaftTestMembers( finalMembers ) ) );
        Assertions.assertEquals( 1, fixture.members().withRole( LEADER ).size(), fixture.messageLog() );
        Assertions.assertEquals( 3, fixture.members().withRole( FOLLOWER ).size(), fixture.messageLog() );
    }

    @Test
    void shouldAddMultipleInstancesToExistingRaftGroup() throws Exception
    {
        // given
        DirectNetworking net = new DirectNetworking();

        final RaftMemberId leader = raftMember( 0 );
        final RaftMemberId stable1 = raftMember( 1 );
        final RaftMemberId stable2 = raftMember( 2 );
        final RaftMemberId toBeAdded1 = raftMember( 3 );
        final RaftMemberId toBeAdded2 = raftMember( 4 );
        final RaftMemberId toBeAdded3 = raftMember( 5 );

        final RaftMemberId[] initialMembers = {leader, stable1, stable2};
        final RaftMemberId[] finalMembers = {leader, stable1, stable2, toBeAdded1, toBeAdded2, toBeAdded3};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, finalMembers );
        fixture.bootstrap( initialMembers );

        fixture.members().withId( leader ).timerService().invoke( ELECTION );
        net.processMessages();

        // when
        fixture.members().setTargetMembershipSet( new RaftTestMembers( finalMembers ).getMembers() );
        net.processMessages();

        // We need a heartbeat for every member we add. It is necessary to have the new members report their state
        // so their membership change can be processed. We can probably do better here.
        fixture.members().withId( leader ).timerService().invoke( HEARTBEAT );
        net.processMessages();
        fixture.members().withId( leader ).timerService().invoke( HEARTBEAT );
        net.processMessages();
        fixture.members().withId( leader ).timerService().invoke( HEARTBEAT );
        net.processMessages();

        // then
        MatcherAssert.assertThat( fixture.messageLog(), fixture.members().withIds( finalMembers ), hasCurrentMembers( new RaftTestMembers( finalMembers ) ) );
        Assertions.assertEquals( 1, fixture.members().withRole( LEADER ).size(), fixture.messageLog() );
        Assertions.assertEquals( 5, fixture.members().withRole( FOLLOWER ).size(), fixture.messageLog() );
    }

    @Test
    void shouldRemoveSingleInstanceFromExistingRaftGroup() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final RaftMemberId leader = raftMember( 0 );
        final RaftMemberId stable = raftMember( 1 );
        final RaftMemberId toBeRemoved = raftMember( 2 );

        final RaftMemberId[] initialMembers = {leader, stable, toBeRemoved};
        final RaftMemberId[] finalMembers = {leader, stable};

        RaftTestFixture fixture = new RaftTestFixture( net, 2, initialMembers );
        fixture.bootstrap( initialMembers );

        fixture.members().withId( leader ).timerService().invoke( ELECTION );

        // when
        fixture.members().setTargetMembershipSet( new RaftTestMembers( finalMembers ).getMembers() );
        net.processMessages();

        // then
        MatcherAssert.assertThat( fixture.messageLog(), fixture.members().withIds( finalMembers ), hasCurrentMembers( new RaftTestMembers( finalMembers ) ) );
        Assertions.assertEquals( 1, fixture.members().withIds( finalMembers ).withRole( LEADER ).size(), fixture.messageLog() );
        Assertions.assertEquals( 1, fixture.members().withIds( finalMembers ).withRole( FOLLOWER ).size(), fixture.messageLog() );
    }

    @Test
    void shouldRemoveMultipleInstancesFromExistingRaftGroup() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final RaftMemberId leader = raftMember( 0 );
        final RaftMemberId stable = raftMember( 1 );
        final RaftMemberId toBeRemoved1 = raftMember( 2 );
        final RaftMemberId toBeRemoved2 = raftMember( 3 );
        final RaftMemberId toBeRemoved3 = raftMember( 4 );

        final RaftMemberId[] initialMembers = {leader, stable, toBeRemoved1, toBeRemoved2, toBeRemoved3};
        final RaftMemberId[] finalMembers = {leader, stable};

        RaftTestFixture fixture = new RaftTestFixture( net, 2, initialMembers );
        fixture.bootstrap( initialMembers );

        fixture.members().withId( leader ).timerService().invoke( ELECTION );
        net.processMessages();

        // when
        fixture.members().withId( leader ).raftInstance().setTargetMembershipSet( new RaftTestMembers( finalMembers ).getMembers() );
        net.processMessages();

        // then
        MatcherAssert.assertThat( fixture.members().withIds( finalMembers ), hasCurrentMembers( new RaftTestMembers( finalMembers ) ) );
        Assertions.assertEquals( 1, fixture.members().withIds( finalMembers ).withRole( LEADER ).size(), fixture.messageLog() );
        Assertions.assertEquals( 1, fixture.members().withIds( finalMembers ).withRole( FOLLOWER ).size(), fixture.messageLog() );
    }

    @Test
    void shouldHandleMixedChangeToExistingRaftGroup() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final RaftMemberId leader = raftMember( 0 );
        final RaftMemberId stable = raftMember( 1 );
        final RaftMemberId toBeRemoved1 = raftMember( 2 );
        final RaftMemberId toBeRemoved2 = raftMember( 3 );
        final RaftMemberId toBeAdded1 = raftMember( 4 );
        final RaftMemberId toBeAdded2 = raftMember( 5 );

        final RaftMemberId[] everyone = {leader, stable, toBeRemoved1, toBeRemoved2, toBeAdded1, toBeAdded2};

        final RaftMemberId[] initialMembers = {leader, stable, toBeRemoved1, toBeRemoved2};
        final RaftMemberId[] finalMembers = {leader, stable, toBeAdded1, toBeAdded2};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, everyone );
        fixture.bootstrap( initialMembers );

        fixture.members().withId( leader ).timerService().invoke( ELECTION );
        net.processMessages();

        // when
        fixture.members().withId( leader ).raftInstance().setTargetMembershipSet(
                new RaftTestMembers( finalMembers ).getMembers() );
        net.processMessages();

        fixture.members().withId( leader ).timerService().invoke( HEARTBEAT );
        net.processMessages();
        fixture.members().withId( leader ).timerService().invoke( HEARTBEAT );
        net.processMessages();
        fixture.members().withId( leader ).timerService().invoke( HEARTBEAT );
        net.processMessages();

        // then
        MatcherAssert.assertThat( fixture.members().withIds( finalMembers ), hasCurrentMembers( new RaftTestMembers( finalMembers ) ) );
        Assertions.assertEquals( 1, fixture.members().withIds( finalMembers ).withRole( LEADER ).size(), fixture.messageLog() );
        Assertions.assertEquals( 3, fixture.members().withIds( finalMembers ).withRole( FOLLOWER ).size(), fixture.messageLog() );
    }

    @Test
    @Disabled // rethink when we implement "safe shutdown"
    void shouldRemoveLeaderFromExistingRaftGroupAndActivelyTransferLeadership() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final RaftMemberId leader = raftMember( 0 );
        final RaftMemberId stable1 = raftMember( 1 );
        final RaftMemberId stable2 = raftMember( 2 );

        final RaftMemberId[] initialMembers = {leader, stable1, stable2};
        final RaftMemberId[] finalMembers = {stable1, stable2};

        RaftTestFixture fixture = new RaftTestFixture( net, 2, initialMembers );
        fixture.bootstrap( initialMembers );
        fixture.members().withId( leader ).timerService().invoke( ELECTION );
        net.processMessages();

        // when
        fixture.members().withId( leader ).raftInstance().setTargetMembershipSet( new RaftTestMembers( finalMembers ).getMembers() );
        net.processMessages();

        fixture.members().withId( stable1 ).timerService().invoke( ELECTION );
        net.processMessages();

        // then
        MatcherAssert.assertThat( fixture.messageLog(), fixture.members().withIds( finalMembers ), hasCurrentMembers( new RaftTestMembers( finalMembers ) ) );
        Assertions.assertTrue( fixture.members().withId( stable1 ).raftInstance().isLeader() ||
                               fixture.members().withId( stable2 ).raftInstance().isLeader(), fixture.messageLog() );
    }

    @Test
    void shouldRemoveLeaderAndAddItBackIn() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final RaftMemberId leader1 = raftMember( 0 );
        final RaftMemberId leader2 = raftMember( 1 );
        final RaftMemberId stable1 = raftMember( 2 );
        final RaftMemberId stable2 = raftMember( 3 );

        final RaftMemberId[] allMembers = {leader1, leader2, stable1, stable2};
        final RaftMemberId[] fewerMembers = {leader2, stable1, stable2};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, allMembers );
        fixture.bootstrap( allMembers );

        // when
        fixture.members().withId( leader1 ).timerService().invoke( ELECTION );
        net.processMessages();

        fixture.members().withId( leader1 ).raftInstance().setTargetMembershipSet( new RaftTestMembers( fewerMembers )
                .getMembers() );
        net.processMessages();

        fixture.members().withId( leader2 ).timerService().invoke( ELECTION );
        net.processMessages();

        fixture.members().withId( leader2 ).raftInstance().setTargetMembershipSet( new RaftTestMembers( allMembers )
                .getMembers() );
        net.processMessages();

        fixture.members().withId( leader2 ).timerService().invoke( HEARTBEAT );
        net.processMessages();

        // then
        Assertions.assertTrue( fixture.members().withId( leader2 ).raftInstance().isLeader(), fixture.messageLog() );
        MatcherAssert.assertThat( fixture.messageLog(), fixture.members().withIds( allMembers ), hasCurrentMembers( new RaftTestMembers( allMembers ) ) );
    }

    @Test
    void shouldRemoveFollowerAndAddItBackIn() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final RaftMemberId leader = raftMember( 0 );
        final RaftMemberId unstable = raftMember( 1 );
        final RaftMemberId stable1 = raftMember( 2 );
        final RaftMemberId stable2 = raftMember( 3 );

        final RaftMemberId[] allMembers = {leader, unstable, stable1, stable2};
        final RaftMemberId[] fewerMembers = {leader, stable1, stable2};

        RaftTestFixture fixture = new RaftTestFixture( net, 3, allMembers );
        fixture.bootstrap( allMembers );

        // when
        fixture.members().withId( leader ).timerService().invoke( ELECTION );
        net.processMessages();

        fixture.members().withId( leader ).raftInstance().setTargetMembershipSet( new RaftTestMembers( fewerMembers ).getMembers() );
        net.processMessages();

        Assertions.assertTrue( fixture.members().withId( leader ).raftInstance().isLeader() );
        MatcherAssert.assertThat( fixture.members().withIds( fewerMembers ), hasCurrentMembers( new RaftTestMembers( fewerMembers ) ) );

        fixture.members().withId( leader ).raftInstance().setTargetMembershipSet( new RaftTestMembers( allMembers ).getMembers() );
        net.processMessages();

        fixture.members().withId( leader ).timerService().invoke( HEARTBEAT );
        net.processMessages();

        // then
        Assertions.assertTrue( fixture.members().withId( leader ).raftInstance().isLeader(), fixture.messageLog() );
        MatcherAssert.assertThat( fixture.messageLog(), fixture.members().withIds( allMembers ), hasCurrentMembers( new RaftTestMembers( allMembers ) ) );
    }

    @Test
    void shouldElectNewLeaderWhenOldOneAbruptlyLeaves() throws Exception
    {
        DirectNetworking net = new DirectNetworking();

        // given
        final RaftMemberId leader1 = raftMember( 0 );
        final RaftMemberId leader2 = raftMember( 1 );
        final RaftMemberId stable = raftMember( 2 );

        final RaftMemberId[] initialMembers = {leader1, leader2, stable};

        RaftTestFixture fixture = new RaftTestFixture( net, 2, initialMembers );
        fixture.bootstrap( initialMembers );

        fixture.members().withId( leader1 ).timerService().invoke( ELECTION );
        net.processMessages();

        // when
        net.disconnect( leader1 );
        fixture.members().withId( leader2 ).timerService().invoke( ELECTION );
        net.processMessages();

        // then
        Assertions.assertTrue( fixture.members().withId( leader2 ).raftInstance().isLeader(), fixture.messageLog() );
        Assertions.assertFalse( fixture.members().withId( stable ).raftInstance().isLeader(), fixture.messageLog() );
        Assertions.assertEquals( 1, fixture.members().withIds( leader2, stable ).withRole( LEADER ).size(), fixture.messageLog() );
        Assertions.assertEquals( 1, fixture.members().withIds( leader2, stable ).withRole( FOLLOWER ).size(), fixture.messageLog() );
    }

    private Matcher<? super RaftTestFixture.Members> hasCurrentMembers( final RaftTestMembers raftGroup )
    {
        return new TypeSafeMatcher<RaftTestFixture.Members>()
        {
            @Override
            protected boolean matchesSafely( RaftTestFixture.Members members )
            {
                for ( RaftTestFixture.MemberFixture finalMember : members )
                {
                    if ( !raftGroup.equals( new RaftTestMembers( finalMember.raftInstance().replicationMembers() ) ) )
                    {
                        return false;
                    }
                }
                return true;
            }

            @Override
            public void describeTo( Description description )
            {
                description.appendText( "Raft group: " ).appendValue( raftGroup );
            }
        };
    }
}
