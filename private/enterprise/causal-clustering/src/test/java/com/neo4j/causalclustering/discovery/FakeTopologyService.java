/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.discovery.akka.database.state.DatabaseToMember;
import com.neo4j.causalclustering.discovery.akka.database.state.ReplicatedDatabaseState;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.dbms.EnterpriseDatabaseState;
import com.neo4j.causalclustering.identity.RaftId;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.util.function.Function.identity;

/**
 * Simple stub topology service with a small number of utility methods and factories for creating stable memberIds from int seeds.
 */
public class FakeTopologyService extends LifecycleAdapter implements TopologyService
{
    private final Map<MemberId,CoreServerInfo> coreMembers;
    private final Map<MemberId,ReadReplicaInfo> replicaMembers;
    //For this test class all members have the same role across all databases
    private final Map<MemberId,RoleInfo> coreRoles;
    private final Map<DatabaseToMember,EnterpriseDatabaseState> databaseStates;
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
                .collect( Collectors.toMap( identity(), ignored -> RoleInfo.FOLLOWER ) );

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
        databaseStates = new HashMap<>();
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

    public void setGroups( Set<MemberId> members, final Set<String> groups )
    {
        Function<CoreServerInfo,CoreServerInfo> coreInfoTransform = serverInfo ->
                new CoreServerInfo( serverInfo.getRaftServer(), serverInfo.catchupServer(),
                        serverInfo.connectors(), groups, serverInfo.databaseIds(), serverInfo.refusesToBeLeader() );

        Function<ReadReplicaInfo,ReadReplicaInfo> replicaInfoTransform = serverInfo ->
                new ReadReplicaInfo( serverInfo.connectors(),
                        serverInfo.catchupServer(), groups, serverInfo.databaseIds() );

        updateMembers( members, coreInfoTransform, replicaInfoTransform );
    }

    public void setDatabases( Set<MemberId> members, final Set<DatabaseId> databases )
    {
        Function<CoreServerInfo,CoreServerInfo> coreInfoTransform = serverInfo ->
                new CoreServerInfo( serverInfo.getRaftServer(), serverInfo.catchupServer(),
                        serverInfo.connectors(), serverInfo.groups(), databases, serverInfo.refusesToBeLeader() );

        Function<ReadReplicaInfo,ReadReplicaInfo> replicaInfoTransform = serverInfo ->
                new ReadReplicaInfo( serverInfo.connectors(),
                        serverInfo.catchupServer(), serverInfo.groups(), databases );

        updateMembers( members, coreInfoTransform, replicaInfoTransform );
    }

    private void updateMembers( Set<MemberId> members, Function<CoreServerInfo,CoreServerInfo> coreInfoTransform,
            Function<ReadReplicaInfo,ReadReplicaInfo> replicaInfoTransform )
    {
        var updatedCores = coreMembers.entrySet().stream()
                .filter( e -> members.contains( e.getKey() ) )
                .collect( Collectors.toMap( Map.Entry::getKey, e -> coreInfoTransform.apply( e.getValue() ) ) );

        var updatedRRs = replicaMembers.entrySet().stream()
                .filter( e -> members.contains( e.getKey() ) )
                .collect( Collectors.toMap( Map.Entry::getKey, e -> replicaInfoTransform.apply( e.getValue() ) ) );

        coreMembers.putAll( updatedCores );
        replicaMembers.putAll( updatedRRs );
    }

    public void setState( Set<MemberId> memberIds, EnterpriseDatabaseState state )
    {
        var newStates = memberIds.stream()
                .collect( Collectors.toMap( m -> new DatabaseToMember( state.databaseId(), m ), ignored -> state ) );
        databaseStates.putAll( newStates );
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
    public void stateChange( DatabaseState newState )
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
        var coresWithDatabase = coreMembers.entrySet().stream()
                .filter( e -> e.getValue().databaseIds().contains( databaseId ) )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
        return new DatabaseCoreTopology( databaseId, RaftId.from( databaseId ), coresWithDatabase );
    }

    @Override
    public Map<MemberId,ReadReplicaInfo> allReadReplicas()
    {
        return replicaMembers;
    }

    @Override
    public DatabaseReadReplicaTopology readReplicaTopologyForDatabase( DatabaseId databaseId )
    {
        var replicasWithDatabase = replicaMembers.entrySet().stream()
                .filter( e -> e.getValue().databaseIds().contains( databaseId ) )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
        return new DatabaseReadReplicaTopology( databaseId, replicasWithDatabase );
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
    public RoleInfo role( DatabaseId ignored, MemberId memberId )
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

    @Override
    public DatabaseState stateFor( DatabaseId databaseId, MemberId memberId )
    {
        return databaseStates.get( new DatabaseToMember( databaseId, memberId ) );
    }

    @Override
    public ReplicatedDatabaseState coreStatesForDatabase( final DatabaseId databaseId )
    {
        var memberStates = getMemberStatesForRole( databaseId, allCoreServers().keySet() );
        return ReplicatedDatabaseState.ofCores( databaseId, memberStates );
    }

    @Override
    public ReplicatedDatabaseState readReplicaStatesForDatabase( DatabaseId databaseId )
    {
        var memberStates = getMemberStatesForRole( databaseId, allReadReplicas().keySet() );
        return ReplicatedDatabaseState.ofCores( databaseId, memberStates );
    }

    private Map<MemberId,DatabaseState> getMemberStatesForRole( final DatabaseId databaseId, Set<MemberId> membersOfRole )
    {
        final var members = Set.copyOf( membersOfRole );
        Predicate<DatabaseToMember> memberIsRoleForDb = key -> Objects.equals( key.databaseId(), databaseId ) && members.contains( key.memberId() );

        return databaseStates.entrySet().stream()
                .filter( e -> memberIsRoleForDb.test( e.getKey() ) )
                .collect( Collectors.toMap( e -> e.getKey().memberId(), Map.Entry::getValue ) );
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
