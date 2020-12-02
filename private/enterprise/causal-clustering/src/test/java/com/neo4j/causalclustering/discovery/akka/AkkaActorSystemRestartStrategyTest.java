/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.actor.Address;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.cluster.UniqueAddress;
import com.neo4j.causalclustering.discovery.RemoteMembersResolver;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import scala.Int;
import scala.collection.immutable.Set;
import scala.collection.immutable.SortedSet;
import scala.collection.immutable.TreeSet;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.configuration.helpers.SocketAddress;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@TestInstance( TestInstance.Lifecycle.PER_METHOD )
public class AkkaActorSystemRestartStrategyTest
{
    private static final int MAX_CONSECUTIVE_FAILURES = 3;
    private static final TreeSet<Member> emptyMemberSet = new TreeSet<>( Member.ordering() );
    private final Cluster cluster = mock( Cluster.class );

    private final SocketAddress selfAddress = new SocketAddress( "localhost", 8000 );
    private final Member self = createMember( selfAddress );
    private final SocketAddress memberOneAddress = new SocketAddress( "localhost", 8001 );
    private final Member memberOne = createMember( memberOneAddress );
    private final SocketAddress memberTwoAddress = new SocketAddress( "otherhost", 8000 );
    private final Member memberTwo = createMember( memberTwoAddress );

    @BeforeEach
    void setUp()
    {
        when( cluster.selfMember() ).thenReturn( self );
    }

    private AkkaActorSystemRestartStrategy.RestartWhenMajorityUnreachableOrSingletonFirstSeed restartStrategy( boolean selfFirstSeed )
    {
        return new AkkaActorSystemRestartStrategy.RestartWhenMajorityUnreachableOrSingletonFirstSeed(
                new TestResolverWithFirstSeed( selfFirstSeed ? selfAddress : memberOneAddress ) );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void singletonFirstSeed( boolean selfFirstSeed )
    {
        var restartStrategy = restartStrategy( selfFirstSeed );
        SortedSet<Member> members = setOf( self );
        Set<Member> unreachableMembers = emptyMemberSet;
        setClusterState( members, unreachableMembers );
        assertThat( restartStrategy.anyRequirementUnsatisfied( cluster ) ).isEqualTo( selfFirstSeed );
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void majorityUnreachable( boolean selfFirstSeed )
    {
        var restartStrategy = restartStrategy( selfFirstSeed );
        SortedSet<Member> members = setOf( self, memberOne, memberTwo );
        Set<Member> unreachableMembers = setOf( memberOne, memberTwo );
        setClusterState( members, unreachableMembers );
        assertThat( restartStrategy.anyRequirementUnsatisfied( cluster ) ).isTrue();
    }

    @ParameterizedTest
    @ValueSource( booleans = {true, false} )
    void minorityUnreachable( boolean selfFirstSeed )
    {
        var restartStrategy = restartStrategy( selfFirstSeed );
        SortedSet<Member> members = setOf( self, memberOne, memberTwo );
        Set<Member> unreachableMembers = setOf( memberOne );
        setClusterState( members, unreachableMembers );
        assertThat( restartStrategy.anyRequirementUnsatisfied( cluster ) ).isFalse();
    }

    @Test
    void shouldFireAfterConsecutiveFailures() throws InterruptedException
    {
        var restartStrategy =
                new AkkaActorSystemRestartStrategy.RestartWhenMajorityUnreachableOrSingletonFirstSeed( new TestResolverWithFirstSeed( selfAddress ) );
        SortedSet<Member> members = setOf( self );
        Set<Member> unreachableMembers = emptyMemberSet;
        setClusterState( members, unreachableMembers );

        assertThat( restartStrategy.anyRequirementUnsatisfied( cluster ) ).isTrue();

        for ( int i = 0; i < MAX_CONSECUTIVE_FAILURES; i++ )
        {
            assertThat( restartStrategy.restartRequired( cluster ) ).isFalse();
            Thread.sleep( restartStrategy.checkFrequency().toMillis() );
        }
        assertThat( restartStrategy.restartRequired( cluster ) ).isTrue();
    }

    @Test
    void shouldFireAfterExpectedTime() throws InterruptedException
    {
        var restartStrategy =
                new AkkaActorSystemRestartStrategy.RestartWhenMajorityUnreachableOrSingletonFirstSeed( new TestResolverWithFirstSeed( selfAddress ) );
        SortedSet<Member> members = setOf( self );
        Set<Member> unreachableMembers = emptyMemberSet;
        setClusterState( members, unreachableMembers );

        assertThat( restartStrategy.anyRequirementUnsatisfied( cluster ) ).isTrue();

        for ( int i = 0; i < MAX_CONSECUTIVE_FAILURES; i++ )
        {
            Assertions.assertThat( restartStrategy.restartRequired( cluster ) ).isFalse();
        }
        Assertions.assertThat( restartStrategy.restartRequired( cluster ) ).isFalse();

        Thread.sleep( restartStrategy.checkFrequency().toMillis() * MAX_CONSECUTIVE_FAILURES );
        Assertions.assertThat( restartStrategy.restartRequired( cluster ) ).isTrue();
    }

    private void setClusterState( SortedSet<Member> members, Set<Member> unreachableMembers )
    {
        when( cluster.state() ).thenReturn( new ClusterEvent.CurrentClusterState( members, unreachableMembers, null, null, null ) );
    }

    private Member createMember( SocketAddress address )
    {
        return new Member( uniqueAddressFor( address ), Int.MaxValue(), MemberStatus.up(), null );
    }

    private UniqueAddress uniqueAddressFor( SocketAddress address )
    {
        return UniqueAddress.apply( Address.apply( "tcp", "akka", address.getHostname(), address.getPort() ), 1 );
    }

    private SortedSet<Member> setOf( Member first, Member... others )
    {
        // Because TreeSet is immutable inserting returns a new TreeSet with the added element.
        var set = emptyMemberSet.insert( first );
        for ( var member : others )
        {
            set = set.insert( member );
        }
        return set;
    }

    private static class TestResolverWithFirstSeed implements RemoteMembersResolver
    {

        private final SocketAddress firstMember;

        private TestResolverWithFirstSeed( SocketAddress firstMember )
        {
            this.firstMember = firstMember;
        }

        @Override
        public <COLL extends Collection<REMOTE>, REMOTE> COLL resolve( Function<SocketAddress,REMOTE> transform, Supplier<COLL> collectionFactory )
        {
            return null;
        }

        @Override
        public boolean useOverrides()
        {
            return false;
        }

        @Override
        public Optional<SocketAddress> first()
        {
            return Optional.of( firstMember );
        }
    }
}
