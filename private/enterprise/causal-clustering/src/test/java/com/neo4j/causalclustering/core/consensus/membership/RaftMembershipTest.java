/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.core.consensus.DirectNetworking;
import com.neo4j.causalclustering.core.consensus.RaftTestFixture;
import com.neo4j.causalclustering.identity.MemberId;
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
import static com.neo4j.causalclustering.identity.RaftTestMember.member;

class RaftMembershipTest
{
    @Test
    void shouldNotFormGroupWithoutAnyBootstrapping()
    {
        // given
        DirectNetworking net = new DirectNetworking();

        final MemberId[] ids = {member( 0 ), member( 1 ), member( 2 )};

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

        final MemberId leader = member( 0 );
        final MemberId stable1 = member( 1 );
        final MemberId stable2 = member( 2 );
        final MemberId toBeAdded = member( 3 );

        final MemberId[] initialMembers = {leader, stable1, stable2};
        final MemberId[] finalMembers = {leader, stable1, stable2, toBeAdded};

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

        final MemberId leader = member( 0 );
        final MemberId stable1 = member( 1 );
        final MemberId stable2 = member( 2 );
        final MemberId toBeAdded1 = member( 3 );
        final MemberId toBeAdded2 = member( 4 );
        final MemberId toBeAdded3 = member( 5 );

        final MemberId[] initialMembers = {leader, stable1, stable2};
        final MemberId[] finalMembers = {leader, stable1, stable2, toBeAdded1, toBeAdded2, toBeAdded3};

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
        final MemberId leader = member( 0 );
        final MemberId stable = member( 1 );
        final MemberId toBeRemoved = member( 2 );

        final MemberId[] initialMembers = {leader, stable, toBeRemoved};
        final MemberId[] finalMembers = {leader, stable};

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
        final MemberId leader = member( 0 );
        final MemberId stable = member( 1 );
        final MemberId toBeRemoved1 = member( 2 );
        final MemberId toBeRemoved2 = member( 3 );
        final MemberId toBeRemoved3 = member( 4 );

        final MemberId[] initialMembers = {leader, stable, toBeRemoved1, toBeRemoved2, toBeRemoved3};
        final MemberId[] finalMembers = {leader, stable};

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
        final MemberId leader = member( 0 );
        final MemberId stable = member( 1 );
        final MemberId toBeRemoved1 = member( 2 );
        final MemberId toBeRemoved2 = member( 3 );
        final MemberId toBeAdded1 = member( 4 );
        final MemberId toBeAdded2 = member( 5 );

        final MemberId[] everyone = {leader, stable, toBeRemoved1, toBeRemoved2, toBeAdded1, toBeAdded2};

        final MemberId[] initialMembers = {leader, stable, toBeRemoved1, toBeRemoved2};
        final MemberId[] finalMembers = {leader, stable, toBeAdded1, toBeAdded2};

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
        final MemberId leader = member( 0 );
        final MemberId stable1 = member( 1 );
        final MemberId stable2 = member( 2 );

        final MemberId[] initialMembers = {leader, stable1, stable2};
        final MemberId[] finalMembers = {stable1, stable2};

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
        final MemberId leader1 = member( 0 );
        final MemberId leader2 = member( 1 );
        final MemberId stable1 = member( 2 );
        final MemberId stable2 = member( 3 );

        final MemberId[] allMembers = {leader1, leader2, stable1, stable2};
        final MemberId[] fewerMembers = {leader2, stable1, stable2};

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
        final MemberId leader = member( 0 );
        final MemberId unstable = member( 1 );
        final MemberId stable1 = member( 2 );
        final MemberId stable2 = member( 3 );

        final MemberId[] allMembers = {leader, unstable, stable1, stable2};
        final MemberId[] fewerMembers = {leader, stable1, stable2};

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
        final MemberId leader1 = member( 0 );
        final MemberId leader2 = member( 1 );
        final MemberId stable = member( 2 );

        final MemberId[] initialMembers = {leader1, leader2, stable};

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
