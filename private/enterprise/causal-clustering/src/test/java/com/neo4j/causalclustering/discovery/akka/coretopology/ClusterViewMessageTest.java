/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

import akka.actor.Address;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.cluster.UniqueAddress;
import co.unruly.matchers.OptionalMatchers;
import co.unruly.matchers.StreamMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.collection.immutable.HashSet;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.empty;

public class ClusterViewMessageTest
{
    @BeforeAll
    static void setUp() throws NoSuchMethodException
    {
        setMemberConstructor( true );
    }

    @AfterAll
    static void tearDown() throws NoSuchMethodException
    {
        setMemberConstructor( false );
    }

    private Member memberup = createMember( 0, MemberStatus.up() );
    private Member memberweaklyUp = createMember( 1, MemberStatus.weaklyUp() );
    private Member memberdown = createMember( 2, MemberStatus.down() );
    private Member memberjoining = createMember( 3, MemberStatus.joining() );
    private Member memberleaving = createMember( 4, MemberStatus.leaving() );
    private Member memberremoved = createMember( 5, MemberStatus.removed() );
    private Member memberexiting = createMember( 6, MemberStatus.exiting() );
    private List<Member> membersOfAllStatuses =
            Arrays.asList( memberup, memberweaklyUp, memberdown, memberjoining, memberleaving, memberremoved, memberexiting );

    //$plus because Scala collection is required.
    private static final HashSet<String> roles = new HashSet<String>().$plus( "dc-foo" );

    public static void setMemberConstructor( boolean accessible ) throws NoSuchMethodException
    {
        Member.class.getConstructor( UniqueAddress.class, int.class, MemberStatus.class, scala.collection.immutable.Set.class ).setAccessible( accessible );
    }

    @Test
    void shouldHaveEmptyAvailableMembersIfNoMembers()
    {
        // given
        ClusterViewMessage clusterView = ClusterViewMessage.EMPTY;

        // then
        MatcherAssert.assertThat( clusterView.availableMembers(), StreamMatchers.empty() );
    }

    @Test
    void shouldIncludeAllMembersInAvailableIfAllReachableAndUp()
    {
        // given
        ClusterViewMessage clusterView = createClusterViewWithMembers( MemberStatus.up() );
        UniqueAddress[] members = clusterView.members().stream().map( Member::uniqueAddress ).toArray( UniqueAddress[]::new );

        // then
        MatcherAssert.assertThat( clusterView.availableMembers(), StreamMatchers.contains( members ) );
    }

    @Test
    void shouldIncludeAllMembersInAvailableIfAllReachableAndWeaklyUp()
    {
        // given
        ClusterViewMessage clusterView = createClusterViewWithMembers( MemberStatus.weaklyUp() );
        UniqueAddress[] members = clusterView.members().stream().map( Member::uniqueAddress ).toArray( UniqueAddress[]::new );

        // then
        MatcherAssert.assertThat( clusterView.availableMembers(), StreamMatchers.contains( members ) );
    }

    @Test
    void shouldExcludeFromAvailableUnreachableMembers()
    {
        // given
        ClusterViewMessage clusterView = createClusterViewWithMembers( MemberStatus.up() );
        int numberUnreachable = 2;
        int limit = numberUnreachable;
        for ( Member member : clusterView.members() )
        {
            if ( limit-- == 0 )
            {
                break;
            }
            clusterView = clusterView.withUnreachable( member );
        }
        UniqueAddress[] reachableMembers = clusterView.members().stream()
                .map( Member::uniqueAddress )
                .skip( numberUnreachable )
                .toArray( UniqueAddress[]::new );

        // then
        MatcherAssert.assertThat( clusterView.availableMembers().count(), Matchers.equalTo( (long) reachableMembers.length ) );
        MatcherAssert.assertThat( clusterView.availableMembers(), StreamMatchers.contains( reachableMembers ) );
    }

    @Test
    void shouldNotIncludeNonUpMembersWhenConstructing()
    {
        // given
        TreeSet<Member> members = new TreeSet<>( Member.ordering() );
        members.addAll( membersOfAllStatuses );

        // when
        ClusterViewMessage clusterView = new ClusterViewMessage( false, members, Collections.emptySet() );

        // then
        MatcherAssert.assertThat( clusterView.members(), Matchers.hasSize( 2 ) );
        MatcherAssert.assertThat( clusterView.members(), Matchers.containsInAnyOrder( memberup, memberweaklyUp ) );
    }

    @Test
    void shouldNotIncludeNonUpMembersWhenAdding()
    {
        // given
        ClusterViewMessage clusterView = ClusterViewMessage.EMPTY;

        // when
        for ( Member member : membersOfAllStatuses )
        {
            clusterView = clusterView.withMember( member );
        }

        // then
        MatcherAssert.assertThat( clusterView.members(), Matchers.hasSize( 2 ) );
        MatcherAssert.assertThat( clusterView.members(), Matchers.containsInAnyOrder( memberup, memberweaklyUp ) );
    }

    @Test
    void shouldBeAbleToAddAnUpMemberThatWasJoiningAtConstruction()
    {
        // given
        Member member = createMember( 1, MemberStatus.joining() );
        TreeSet<Member> members = new TreeSet<>( Member.ordering() );
        members.add( member );
        ClusterViewMessage clusterView = new ClusterViewMessage( false, members, Collections.emptySet() );
        Member memberUp = new Member( member.uniqueAddress(), member.upNumber(), MemberStatus.up(), roles );

        // when
        ClusterViewMessage modifiedClusterView = clusterView.withMember( memberUp );

        // then
        MatcherAssert.assertThat( modifiedClusterView.members(), Matchers.contains( memberUp ) );
    }

    @Test
    void shouldBeAbleToUpdateAMemberFromWeaklyUpToUp()
    {
        // given
        ClusterViewMessage clusterView = ClusterViewMessage.EMPTY.withMember( memberweaklyUp );

        Member memberUp = new Member( memberweaklyUp.uniqueAddress(), memberweaklyUp.upNumber(), MemberStatus.up(), roles );

        // when
        ClusterViewMessage result = clusterView.withMember( memberUp );

        // then
        MatcherAssert.assertThat( result.members(), Matchers.contains( memberUp ) );
        Optional<Member> returnedMember = result.members().stream().filter( m -> m.equals( memberUp ) ).findFirst();
        MatcherAssert.assertThat( returnedMember.map( Member::status ), OptionalMatchers.contains( Matchers.equalTo( MemberStatus.up() ) ) );
    }

    @Test
    void shouldNotIncludeInUnreachableWhenUpdatingFromWeaklyUpToUp()
    {
        // given
        ClusterViewMessage clusterView = ClusterViewMessage.EMPTY.withMember( memberweaklyUp );

        Member memberUp = new Member( memberweaklyUp.uniqueAddress(), memberweaklyUp.upNumber(), MemberStatus.up(), roles );

        // when
        ClusterViewMessage result = clusterView.withMember( memberUp );

        // then
        MatcherAssert.assertThat( result.unreachable(), empty() );
    }

    @Test
    void shouldBeAbleToUpdateUnreachableMemberFromWeaklyUpToUp()
    {
        // given
        ClusterViewMessage initial = ClusterViewMessage.EMPTY
                .withMember( memberweaklyUp )
                .withUnreachable( memberweaklyUp );

        Member memberUp = new Member( memberweaklyUp.uniqueAddress(), memberweaklyUp.upNumber(), MemberStatus.up(), roles );

        // when
        ClusterViewMessage result = initial.withMember( memberUp );

        // then
        MatcherAssert.assertThat( result.unreachable(), Matchers.contains( memberUp ) );
        Optional<Member> returnedMember = result.unreachable().stream().filter( m -> m.equals( memberUp ) ).findFirst();
        MatcherAssert.assertThat( returnedMember.map( Member::status ), OptionalMatchers.contains( Matchers.equalTo( MemberStatus.up() ) ) );
    }

    @Test
    void shouldRemoveFromUnreachableWhenRemovingFromView()
    {
        // given
        ClusterViewMessage initial = ClusterViewMessage.EMPTY
                .withMember( memberup )
                .withUnreachable( memberup );

        // when
        ClusterViewMessage result = initial.withoutMember( memberup );

        // then
        MatcherAssert.assertThat( result.unreachable(), empty() );
    }

    private ClusterViewMessage createClusterViewWithMembers( MemberStatus status )
    {
        SortedSet<Member> members = IntStream.range( 0, 5 )
                .mapToObj( port -> createMember( port, status ) )
                .collect( Collectors.toCollection( () -> new TreeSet<>( Member.ordering() ) ) );

        return new ClusterViewMessage( false, members, Collections.emptySet() );
    }

    static Member createMember( int port, MemberStatus status )
    {
        return createMember( new UniqueAddress( new Address( "protocol", "system", "host", port ), 0L ), status );
    }

    private static Member createMember( UniqueAddress uniqueAddress, MemberStatus status )
    {
        return new Member( uniqueAddress, 0, status, roles );
    }
}
