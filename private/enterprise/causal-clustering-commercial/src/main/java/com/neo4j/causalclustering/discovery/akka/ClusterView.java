/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import akka.cluster.ClusterEvent;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import akka.cluster.UniqueAddress;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.util.VisibleForTesting;

import static java.util.Collections.unmodifiableSet;
import static java.util.Collections.unmodifiableSortedSet;

public class ClusterView
{
    private final boolean converged;
    private final SortedSet<Member> members;
    private final Set<Member> unreachable;

    public static final ClusterView EMPTY = new ClusterView();

    private ClusterView()
    {
        this( false, unmodifiableSortedSet( new TreeSet<>( Member.ordering() ) ), unmodifiableSet( new HashSet<>() ) );
    }

    public ClusterView( ClusterEvent.CurrentClusterState clusterState )
    {
        this( clusterState.leader().isDefined(), membersFrom( clusterState ), clusterState.getUnreachable() );
    }

    private static SortedSet<Member> membersFrom( ClusterEvent.CurrentClusterState clusterState )
    {
        TreeSet<Member> tempMembers = new TreeSet<>( Member.ordering() );
        clusterState.getMembers().forEach( tempMembers::add );
        return tempMembers;
    }

    @VisibleForTesting
    ClusterView( boolean converged, SortedSet<Member> members, Set<Member> unreachable )
    {
        this.converged = converged;
        TreeSet<Member> upMembers = members.stream()
                .filter( this::memberIsUp )
                .collect( Collectors.toCollection( () -> new TreeSet<>( Member.ordering() ) ) );
        this.members = unmodifiableSortedSet( upMembers );
        this.unreachable = unmodifiableSet( unreachable );
    }

    public boolean converged()
    {
        return converged;
    }

    public ClusterView withConverged( boolean converged )
    {
        return new ClusterView( converged, members, unreachable );
    }

    @VisibleForTesting
    public SortedSet<Member> members()
    {
        return members;
    }

    public ClusterView withMember( Member member )
    {
        TreeSet<Member> tempMembers = new TreeSet<>( Member.ordering() );
        tempMembers.addAll( members );
        tempMembers.remove( member ); // To update
        tempMembers.add( member );
        return new ClusterView( converged, tempMembers, unreachable );
    }

    public ClusterView withoutMember( Member member )
    {
        TreeSet<Member> tempMembers = new TreeSet<>( Member.ordering() );
        tempMembers.addAll( members );
        tempMembers.remove( member );
        return new ClusterView( converged, tempMembers, unreachable );
    }

    public ClusterView withUnreachable( Member member )
    {
        Set<Member> tempUnreachable = new HashSet<>( unreachable );
        tempUnreachable.add( member );
        return new ClusterView( converged, members, tempUnreachable );
    }

    public ClusterView withoutUnreachable( Member member )
    {
        Set<Member> tempUnreachable = new HashSet<>( unreachable );
        tempUnreachable.remove( member );
        return new ClusterView( converged, members, tempUnreachable );
    }

    public Stream<UniqueAddress> availableMembers()
    {
        return members.stream()
                .filter( member -> !unreachable.contains( member ) )
                .map( Member::uniqueAddress );
    }

    private boolean memberIsUp( Member m )
    {
        return MemberStatus.up().equals( m.status() ) || MemberStatus.weaklyUp().equals( m.status() );
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        ClusterView that = (ClusterView) o;
        return converged == that.converged && Objects.equals( members, that.members ) && Objects.equals( unreachable, that.unreachable );
    }

    @Override
    public int hashCode()
    {

        return Objects.hash( converged, members, unreachable );
    }

    @Override
    public String toString()
    {
        return "ClusterView{" + "converged=" + converged + ", members=" + members + ", unreachable=" + unreachable + '}';
    }
}
