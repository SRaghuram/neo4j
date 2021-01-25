/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.coretopology;

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

public class ClusterViewMessage
{
    private final boolean converged;
    private final SortedSet<Member> members;
    private final Set<Member> unreachable;

    public static final ClusterViewMessage EMPTY = new ClusterViewMessage();

    private ClusterViewMessage()
    {
        this( false, unmodifiableSortedSet( new TreeSet<>( Member.ordering() ) ), unmodifiableSet( new HashSet<>() ) );
    }

    public ClusterViewMessage( ClusterEvent.CurrentClusterState clusterState )
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
    ClusterViewMessage( boolean converged, SortedSet<Member> members, Set<Member> unreachable )
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

    public ClusterViewMessage withConverged( boolean converged )
    {
        return new ClusterViewMessage( converged, members, unreachable );
    }

    @VisibleForTesting
    public SortedSet<Member> members()
    {
        return members;
    }

    /**
     * If members or unreachable contains the same member with a different status it will be replaced.
     *
     * Note that {@link Member#equals(Object)} only compares its {@link UniqueAddress}, so this method
     * replaces by removing and then adding.
     */
    public ClusterViewMessage withMember( Member member )
    {
        TreeSet<Member> tempMembers = new TreeSet<>( Member.ordering() );
        tempMembers.addAll( members );
        tempMembers.remove( member );
        tempMembers.add( member );

        Set<Member> tempUnreachable = new HashSet<>( unreachable );
        if ( tempUnreachable.remove( member ) )
        {
            tempUnreachable.add( member );
        }
        return new ClusterViewMessage( converged, tempMembers, tempUnreachable );
    }

    public ClusterViewMessage withoutMember( Member member )
    {
        TreeSet<Member> tempMembers = new TreeSet<>( Member.ordering() );
        tempMembers.addAll( members );
        tempMembers.remove( member );
        Set<Member> tempUnreachable = new HashSet<>( unreachable );
        tempUnreachable.remove( member );
        return new ClusterViewMessage( converged, tempMembers, tempUnreachable );
    }

    public ClusterViewMessage withUnreachable( Member member )
    {
        Set<Member> tempUnreachable = new HashSet<>( unreachable );
        tempUnreachable.add( member );
        return new ClusterViewMessage( converged, members, tempUnreachable );
    }

    public ClusterViewMessage withoutUnreachable( Member member )
    {
        Set<Member> tempUnreachable = new HashSet<>( unreachable );
        tempUnreachable.remove( member );
        return new ClusterViewMessage( converged, members, tempUnreachable );
    }

    public Stream<UniqueAddress> availableMembers()
    {
        return members.stream()
                .filter( member -> !unreachable.contains( member ) )
                .map( Member::uniqueAddress );
    }

    public Set<Member> unreachable()
    {
        return unreachable;
    }

    public boolean mostAreReachable()
    {
        int unreachableSize = this.unreachable.size();
        int reachableSize = members.size() - unreachableSize; // members are all Up or WeaklyUp

        return reachableSize > unreachableSize;
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
        ClusterViewMessage that = (ClusterViewMessage) o;
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
        return "ClusterViewMessage{" + "converged=" + converged + ", members=" + members + ", unreachable=" + unreachable + '}';
    }
}
