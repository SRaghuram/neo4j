/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Simple stub topology service with a small number of utility methods and factories for creating stable memberIds from int seeds.
 */
public class FakeTopologyService extends LifecycleAdapter implements TopologyService
{
    private final Map<MemberId,CoreServerInfo> coreMembers;
    private final Map<MemberId,ReadReplicaInfo> replicaMembers;
    //For this test class all members have the same role across all databases
    private final Map<MemberId,RoleInfo> coreRoles;
    private final MemberId myself;

    public FakeTopologyService( Set<MemberId> cores, Set<MemberId> replicas, MemberId myself, Set<DatabaseId> databaseIds )
    {
        this.myself = myself;

        int offset = 0;
        this.coreMembers = new HashMap<>();
        for ( MemberId core : cores )
        {
            coreMembers.put( core, TestTopology.addressesForCore( offset, false, databaseIds ) );
            offset++;
        }

        this.coreRoles = this.coreMembers.keySet().stream()
                .map( memberId -> Pair.of( memberId, RoleInfo.FOLLOWER ) )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );

        var candidates = new ArrayList<>( cores );

        if ( candidates.size() > 0 )
        {
            Collections.shuffle( candidates );
            var leader = candidates.get( 0 );
            coreRoles.put( leader, RoleInfo.LEADER );
        }

        this.replicaMembers = new HashMap<>();
        for ( MemberId replica : replicas )
        {
            replicaMembers.put( replica, TestTopology.addressesForReadReplica( offset, databaseIds ) );
            offset++;
        }
    }

    public void setRole( MemberId memberId, RoleInfo nextRole )
    {
        if ( memberId == null && nextRole == RoleInfo.LEADER )
        {
            leader().ifPresent( member -> coreRoles.put( member, RoleInfo.FOLLOWER ) );
            return;
        }
        else if ( !coreRoles.containsKey( memberId ) )
        {
            return;
        }

        var currentRole = coreRoles.get( memberId );

        if ( currentRole != nextRole && nextRole == RoleInfo.LEADER )
        {
            leader().ifPresent( member -> coreRoles.put( member, RoleInfo.FOLLOWER ) );
        }
        coreRoles.put( memberId, nextRole );
    }

    private Optional<MemberId> leader()
    {
        return coreRoles.entrySet().stream()
                .filter( e -> e.getValue() == RoleInfo.LEADER )
                .map( Map.Entry::getKey )
                .findAny();
    }

    public void setGroups( Set<MemberId> members, Set<String> groups )
    {
        var updatedCores = coreMembers.entrySet().stream()
                .filter( e -> members.contains( e.getKey() ) )
                .map( e ->
                {
                    var serverInfo = e.getValue();
                    return Pair.of( e.getKey(), new CoreServerInfo( serverInfo.getRaftServer(), serverInfo.catchupServer(),
                            serverInfo.connectors(), groups, serverInfo.getDatabaseIds(), serverInfo.refusesToBeLeader() ) );
                } )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );

        var updatedRRs = replicaMembers.entrySet().stream()
                .filter( e -> members.contains( e.getKey() ) )
                .map( e ->
                {
                    var serverInfo = e.getValue();
                    return Pair.of( e.getKey(), new ReadReplicaInfo( serverInfo.connectors(),
                            serverInfo.catchupServer(), groups, serverInfo.getDatabaseIds() ) );
                } )
                .collect( Collectors.toMap( Pair::first, Pair::other ) );

        coreMembers.putAll( updatedCores );
        replicaMembers.putAll( updatedRRs );
    }

    @Override
    public void onDatabaseStart( DatabaseId databaseId )
    {
    }

    @Override
    public void onDatabaseStop( DatabaseId databaseId )
    {
    }

    @Override
    public Map<MemberId,CoreServerInfo> allCoreServers()
    {
        return coreMembers;
    }

    @Override
    public DatabaseCoreTopology coreTopologyForDatabase( DatabaseId databaseId )
    {
        return new DatabaseCoreTopology( databaseId, RaftId.from( databaseId ), coreMembers );
    }

    @Override
    public Map<MemberId,ReadReplicaInfo> allReadReplicas()
    {
        return replicaMembers;
    }

    @Override
    public DatabaseReadReplicaTopology readReplicaTopologyForDatabase( DatabaseId databaseId )
    {
        return new DatabaseReadReplicaTopology( databaseId, replicaMembers );
    }

    @Override
    public SocketAddress findCatchupAddress( MemberId upstream ) throws CatchupAddressResolutionException
    {
        return Optional.<DiscoveryServerInfo>ofNullable( coreMembers.get( upstream ) )
                .or( () -> Optional.ofNullable( replicaMembers.get( upstream ) ) )
                .map( DiscoveryServerInfo::catchupServer )
                .orElseThrow( () -> new CatchupAddressResolutionException( upstream ) );
    }

    @Override
    public RoleInfo coreRole( DatabaseId ignored, MemberId memberId )
    {
        var role = coreRoles.get( memberId );
        if ( role == null )
        {
            return RoleInfo.UNKNOWN;
        }
        return role;
    }

    @Override
    public MemberId memberId()
    {
        return myself;
    }

    public static MemberId memberId( int seed )
    {
        var rng = new Random( seed );
        return new MemberId( new UUID( rng.nextLong(), rng.nextLong() ) );
    }

    public static Set<MemberId> memberIds( int from, int until )
    {
        return IntStream.range( from, until ).mapToObj( FakeTopologyService::memberId ).collect( Collectors.toSet() );
    }
}
