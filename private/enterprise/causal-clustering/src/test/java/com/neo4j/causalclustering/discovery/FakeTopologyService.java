/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.akka.database.state.DatabaseServer;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.configuration.ServerGroupName;

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

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static java.util.function.Function.identity;

/**
 * Simple stub topology service with a small number of utility methods and factories for creating stable memberIds from int seeds.
 */
public class FakeTopologyService extends LifecycleAdapter implements TopologyService
{
    private final Map<ServerId,CoreServerInfo> coreMembers;
    private final Map<ServerId,ReadReplicaInfo> replicaMembers;
    //For this test class all members have the same role across all databases
    private final Map<ServerId,RoleInfo> coreRoles;
    private final Map<DatabaseServer,DiscoveryDatabaseState> databaseStates;
    private final ServerId myself;
    private final Map<RaftMemberId,ServerId> raftMemberIdMapping = new HashMap<>();

    public FakeTopologyService( Set<ServerId> cores, Set<ServerId> replicas, ServerId myself, Set<NamedDatabaseId> namedDatabaseIds )
    {
        this.myself = myself;

        int offset = 0;
        this.coreMembers = new HashMap<>();
        var databaseIds = namedDatabaseIds.stream().map( NamedDatabaseId::databaseId ).collect( Collectors.toSet() );
        for ( ServerId core : cores )
        {
            coreMembers.put( core, TestTopology.addressesForCore( offset, databaseIds ) );
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
        for ( ServerId replica : replicas )
        {
            replicaMembers.put( replica, TestTopology.addressesForReadReplica( offset, databaseIds ) );
            offset++;
        }
        databaseStates = new HashMap<>();
    }

    public void setRole( ServerId serverId, RoleInfo nextRole )
    {
        if ( serverId == null && nextRole == RoleInfo.LEADER )
        {
            leader().ifPresent( member -> coreRoles.put( member, RoleInfo.FOLLOWER ) );
            return;
        }
        else if ( !coreRoles.containsKey( serverId ) )
        {
            return;
        }

        var currentRole = coreRoles.get( serverId );

        if ( currentRole != nextRole && nextRole == RoleInfo.LEADER )
        {
            leader().ifPresent( member -> coreRoles.put( member, RoleInfo.FOLLOWER ) );
        }
        coreRoles.put( serverId, nextRole );
    }

    private Optional<ServerId> leader()
    {
        return coreRoles.entrySet().stream()
                .filter( e -> e.getValue() == RoleInfo.LEADER )
                .map( Map.Entry::getKey )
                .findAny();
    }

    public void setGroups( Set<ServerId> members, final Set<ServerGroupName> groups )
    {
        Function<CoreServerInfo,CoreServerInfo> coreInfoTransform = serverInfo ->
                new CoreServerInfo( serverInfo.getRaftServer(), serverInfo.catchupServer(),
                        serverInfo.connectors(), groups, serverInfo.startedDatabaseIds() );

        Function<ReadReplicaInfo,ReadReplicaInfo> replicaInfoTransform = serverInfo ->
                new ReadReplicaInfo( serverInfo.connectors(),
                        serverInfo.catchupServer(), groups, serverInfo.startedDatabaseIds() );

        updateMembers( members, coreInfoTransform, replicaInfoTransform );
    }

    public void setDatabases( Set<ServerId> members, final Set<DatabaseId> databases )
    {
        Function<CoreServerInfo,CoreServerInfo> coreInfoTransform = serverInfo ->
                new CoreServerInfo( serverInfo.getRaftServer(), serverInfo.catchupServer(),
                        serverInfo.connectors(), serverInfo.groups(), databases );

        Function<ReadReplicaInfo,ReadReplicaInfo> replicaInfoTransform = serverInfo ->
                new ReadReplicaInfo( serverInfo.connectors(),
                        serverInfo.catchupServer(), serverInfo.groups(), databases );

        updateMembers( members, coreInfoTransform, replicaInfoTransform );
    }

    private void updateMembers( Set<ServerId> servers, Function<CoreServerInfo,CoreServerInfo> coreInfoTransform,
            Function<ReadReplicaInfo,ReadReplicaInfo> replicaInfoTransform )
    {
        var updatedCores = coreMembers.entrySet().stream()
                .filter( e -> servers.contains( e.getKey() ) )
                .collect( Collectors.toMap( Map.Entry::getKey, e -> coreInfoTransform.apply( e.getValue() ) ) );

        var updatedRRs = replicaMembers.entrySet().stream()
                .filter( e -> servers.contains( e.getKey() ) )
                .collect( Collectors.toMap( Map.Entry::getKey, e -> replicaInfoTransform.apply( e.getValue() ) ) );

        coreMembers.putAll( updatedCores );
        replicaMembers.putAll( updatedRRs );
    }

    public void setState( Set<ServerId> memberIds, DiscoveryDatabaseState state )
    {
        var newStates = memberIds.stream()
                .collect( Collectors.toMap( m -> new DatabaseServer( state.databaseId(), m ), ignored -> state ) );
        databaseStates.putAll( newStates );
    }

    @Override
    public void onDatabaseStart( NamedDatabaseId namedDatabaseId )
    {
    }

    @Override
    public void onRaftMemberKnown( NamedDatabaseId namedDatabaseId )
    {
    }

    @Override
    public void onDatabaseStop( NamedDatabaseId namedDatabaseId )
    {
    }

    @Override
    public void stateChange( DatabaseState previousState, DatabaseState newState )
    {
    }

    @Override
    public Map<ServerId,CoreServerInfo> allCoreServers()
    {
        return coreMembers;
    }

    @Override
    public DatabaseCoreTopology coreTopologyForDatabase( NamedDatabaseId namedDatabaseId )
    {
        var databaseId = namedDatabaseId.databaseId();
        var coresWithDatabase = coreMembers.entrySet().stream()
                .filter( e -> e.getValue().startedDatabaseIds().contains( databaseId ) )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
        return new DatabaseCoreTopology( databaseId, RaftGroupId.from( databaseId ), coresWithDatabase );
    }

    @Override
    public Map<ServerId,ReadReplicaInfo> allReadReplicas()
    {
        return replicaMembers;
    }

    @Override
    public DatabaseReadReplicaTopology readReplicaTopologyForDatabase( NamedDatabaseId namedDatabaseId )
    {
        var databaseId = namedDatabaseId.databaseId();
        var replicasWithDatabase = replicaMembers.entrySet().stream()
                .filter( e -> e.getValue().startedDatabaseIds().contains( databaseId ) )
                .collect( Collectors.toMap( Map.Entry::getKey, Map.Entry::getValue ) );
        return new DatabaseReadReplicaTopology( databaseId, replicasWithDatabase );
    }

    @Override
    public SocketAddress lookupCatchupAddress( ServerId upstream ) throws CatchupAddressResolutionException
    {
        return Optional.<DiscoveryServerInfo>ofNullable( coreMembers.get( upstream ) )
                .or( () -> Optional.ofNullable( replicaMembers.get( upstream ) ) )
                .map( DiscoveryServerInfo::catchupServer )
                .orElseThrow( () -> new CatchupAddressResolutionException( upstream ) );
    }

    @Override
    public RoleInfo lookupRole( NamedDatabaseId ignored, ServerId serverId )
    {
        var role = coreRoles.get( serverId );
        if ( role == null )
        {
            var isReadReplica = replicaMembers.containsKey( serverId );
            return isReadReplica ? RoleInfo.READ_REPLICA : RoleInfo.UNKNOWN;
        }
        return role;
    }

    @Override
    public LeaderInfo getLeader( NamedDatabaseId namedDatabaseId )
    {
        return coreRoles.entrySet().stream()
                        .filter( entry -> entry.getValue() == RoleInfo.LEADER )
                        .map( entry -> new LeaderInfo( new RaftMemberId( entry.getKey().uuid() ), 1 ) )
                        .findFirst().orElse( null );
    }

    @Override
    public RaftMemberId resolveRaftMemberForServer( NamedDatabaseId namedDatabaseId, ServerId serverId )
    {
        return new RaftMemberId( serverId.uuid() );
    }

    @Override
    public ServerId resolveServerForRaftMember( RaftMemberId raftMemberId )
    {
        return raftMemberIdMapping.getOrDefault( raftMemberId, new ServerId( raftMemberId.uuid() ) );
    }

    public void removeLeader()
    {
        coreRoles.entrySet()
                 .stream()
                 .filter( entry -> entry.getValue() == RoleInfo.LEADER )
                 .findFirst()
                 .ifPresent( leader -> coreRoles.remove( leader.getKey() ) );
    }

    @Override
    public ServerId serverId()
    {
        return myself;
    }

    @Override
    public DiscoveryDatabaseState lookupDatabaseState( NamedDatabaseId namedDatabaseId, ServerId serverId )
    {
        return databaseStates.get( new DatabaseServer( namedDatabaseId.databaseId(), serverId ) );
    }

    @Override
    public Map<ServerId,DiscoveryDatabaseState> allCoreStatesForDatabase( final NamedDatabaseId namedDatabaseId )
    {
        return getMemberStatesForRole( namedDatabaseId, allCoreServers().keySet() );
    }

    @Override
    public Map<ServerId,DiscoveryDatabaseState> allReadReplicaStatesForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return getMemberStatesForRole( namedDatabaseId, allReadReplicas().keySet() );
    }

    @Override
    public boolean isHealthy()
    {
        return true;
    }

    private Map<ServerId,DiscoveryDatabaseState> getMemberStatesForRole( final NamedDatabaseId namedDatabaseId, Set<ServerId> membersOfRole )
    {
        final var members = Set.copyOf( membersOfRole );
        Predicate<DatabaseServer> memberIsRoleForDb =
                key -> Objects.equals( key.databaseId(), namedDatabaseId.databaseId() ) && members.contains( key.serverId() );

        return databaseStates.entrySet().stream()
                .filter( e -> memberIsRoleForDb.test( e.getKey() ) )
                .collect( Collectors.toMap( e -> e.getKey().serverId(), Map.Entry::getValue ) );
    }

    public static ServerId serverId( int seed )
    {
        var rng = new Random( seed );
        return new ServerId( new UUID( rng.nextLong(), rng.nextLong() ) );
    }

    public static Set<ServerId> serverIds( int from, int until )
    {
        return IntStream.range( from, until ).mapToObj( FakeTopologyService::serverId ).collect( Collectors.toSet() );
    }

    public void setRaftMemberIdMapping( RaftMemberId raftMemberId, ServerId serverId )
    {
        raftMemberIdMapping.put( raftMemberId, serverId );
    }
}
