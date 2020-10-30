/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.core.consensus.schedule.TimerService;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.discovery.ReplicatedRaftMapping;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.Topology;
import com.neo4j.causalclustering.discovery.akka.coretopology.BootstrapState;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

import static java.lang.String.format;
import static java.lang.System.lineSeparator;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static java.util.Optional.ofNullable;
import static org.neo4j.internal.helpers.Strings.printMap;

public class GlobalTopologyState implements TopologyUpdateSink, DirectoryUpdateSink, BootstrapStateUpdateSink, DatabaseStateUpdateSink, RaftMappingUpdateSink
{
    private final Log log;
    private final BiConsumer<DatabaseId,Set<RaftMemberId>> callback;
    private final TopologyLogger topologyLogger;
    private volatile Map<ServerId,CoreServerInfo> coresByServerId;
    private volatile Map<ServerId,ReadReplicaInfo> readReplicasByServerId;
    private volatile Map<DatabaseId,LeaderInfo> remoteDbLeaderMap;
    private volatile BootstrapState bootstrapState = BootstrapState.EMPTY;
    private final Map<DatabaseId,DatabaseCoreTopology> coreTopologiesByDatabase = new ConcurrentHashMap<>();
    private final Map<DatabaseId,DatabaseReadReplicaTopology> readReplicaTopologiesByDatabase = new ConcurrentHashMap<>();
    private final Map<DatabaseId,ReplicatedDatabaseState> coreStatesByDatabase = new ConcurrentHashMap<>();
    private final Map<DatabaseId,ReplicatedDatabaseState> readReplicaStatesByDatabase = new ConcurrentHashMap<>();
    private final RaftMappingState raftMappingState;

    GlobalTopologyState( LogProvider logProvider, BiConsumer<DatabaseId,Set<RaftMemberId>> listener, JobScheduler jobScheduler )
    {
        var timerService = new TimerService( jobScheduler, logProvider );
        this.log = logProvider.getLog( getClass() );
        this.coresByServerId = emptyMap();
        this.readReplicasByServerId = emptyMap();
        this.remoteDbLeaderMap = emptyMap();
        this.callback = listener;
        this.raftMappingState = new RaftMappingState( log );
        this.topologyLogger = new TopologyLogger( timerService, logProvider, getClass(), this::getAllDatabases );
    }

    @Override
    public void onTopologyUpdate( DatabaseCoreTopology newCoreTopology )
    {
        var databaseId = newCoreTopology.databaseId();
        DatabaseCoreTopology currentCoreTopology = coreTopologiesByDatabase.put( databaseId, newCoreTopology );

        if ( !Objects.equals( currentCoreTopology, newCoreTopology ) )
        {
            if ( currentCoreTopology == null )
            {
                currentCoreTopology = DatabaseCoreTopology.empty( databaseId );
            }
            this.coresByServerId = extractServerInfos( coreTopologiesByDatabase );
            topologyLogger.logChange( "core topology", newCoreTopology, currentCoreTopology );
            this.coresByServerId = extractServerInfos( coreTopologiesByDatabase );
            topologyLogger.logChange( "Core topology", newCoreTopology, currentCoreTopology );
            callback.accept( databaseId, newCoreTopology.members( this::resolveRaftMemberForServer ) );
        }

        if ( hasNoMembers( newCoreTopology ) )
        {
            // do not store core topologies with no members, they can represent dropped databases
            coreTopologiesByDatabase.remove( databaseId, newCoreTopology );
        }
    }

    @Override
    public void onTopologyUpdate( DatabaseReadReplicaTopology newReadReplicaTopology )
    {
        var databaseId = newReadReplicaTopology.databaseId();
        DatabaseReadReplicaTopology currentReadReplicaTopology = readReplicaTopologiesByDatabase.put( databaseId, newReadReplicaTopology );

        if ( !Objects.equals( currentReadReplicaTopology, newReadReplicaTopology ) )
        {
            if ( currentReadReplicaTopology == null )
            {
                currentReadReplicaTopology = DatabaseReadReplicaTopology.empty( databaseId );
            }
            this.readReplicasByServerId = extractServerInfos( readReplicaTopologiesByDatabase );
            topologyLogger.logChange( "read replica topology", newReadReplicaTopology, currentReadReplicaTopology );
        }

        if ( hasNoMembers( newReadReplicaTopology ) )
        {
            // do not store read replica topologies with no members, they can represent dropped databases
            readReplicaTopologiesByDatabase.remove( databaseId, newReadReplicaTopology );
        }
    }

    @Override
    public void onDbLeaderUpdate( Map<DatabaseId,LeaderInfo> leaderInfoMap )
    {
        if ( !leaderInfoMap.equals( remoteDbLeaderMap ) )
        {
            log.info( "Database leader(s) update:%s%s", newPaddedLine(), printLeaderInfoMap( leaderInfoMap, remoteDbLeaderMap ) );
            this.remoteDbLeaderMap = leaderInfoMap;
        }
    }

    @Override
    public void onDbStateUpdate( ReplicatedDatabaseState newState )
    {
        var role = newState.containsCoreStates() ? "core" : "read_replica";
        var statesByDatabase = newState.containsCoreStates() ? coreStatesByDatabase : readReplicaStatesByDatabase;
        var databaseId = newState.databaseId();
        var previousState = statesByDatabase.put( databaseId, newState );

        if ( !Objects.equals( previousState, newState ) )
        {
            StringBuilder stringBuilder =
                    new StringBuilder( format( "The %s replicated states for database %s changed", role, databaseId ) ).append( lineSeparator() );
            if ( previousState == null )
            {
                stringBuilder.append( "previous state was empty" );
            }
            else
            {
                stringBuilder.append( "previous state was:" ).append( newPaddedLine() ).append( printMap( previousState.memberStates(), newPaddedLine() ) );
            }
            stringBuilder.append( lineSeparator() );
            if ( newState.isEmpty() )
            {
                stringBuilder.append( "current state is empty" );
            }
            else
            {
                stringBuilder.append( "current state is:" ).append( newPaddedLine() ).append( printMap( newState.memberStates(), newPaddedLine() ) );
            }
            log.info( stringBuilder.toString() );
        }

        if ( newState.isEmpty() )
        {
            // do not store replicated states with no members left, they can represent dropped databases
            statesByDatabase.remove( databaseId, newState );
        }
    }

    @Override
    public void onBootstrapStateUpdate( BootstrapState newBootstrapState )
    {
        bootstrapState = requireNonNull( newBootstrapState );
    }

    @Override
    public void onRaftMappingUpdate( ReplicatedRaftMapping mapping )
    {
        var changedDatabaseIds = raftMappingState.update( mapping );
        changedDatabaseIds.stream().filter( Objects::nonNull ).map( coreTopologiesByDatabase::get ).filter( Objects::nonNull )
                .forEach( newCoreTopology -> callback.accept( newCoreTopology.databaseId(), newCoreTopology.members( this::resolveRaftMemberForServer ) ) );
    }

    private static String printLeaderInfoMap( Map<DatabaseId,LeaderInfo> leaderInfoMap, Map<DatabaseId,LeaderInfo> oldDbLeaderMap )
    {
        var allDatabaseIds = new HashSet<>( leaderInfoMap.keySet() );
        allDatabaseIds.addAll( oldDbLeaderMap.keySet() );

        return allDatabaseIds.stream().map( dbId ->
        {
            var oldLeader = oldDbLeaderMap.get( dbId );
            var newLeader = leaderInfoMap.get( dbId );
            if ( oldLeader == null )
            {
                if ( newLeader == null )
                {
                    return format( "No leader for database %s", dbId );
                }
                return format( "Discovered leader %s in term %d for database %s", newLeader.memberId(), newLeader.term(), dbId );
            }
            if ( newLeader == null )
            {
                return format( "Database %s lost its leader. Previous leader was %s", dbId, oldLeader.memberId() );
            }
            if ( !Objects.equals( newLeader.memberId(), oldLeader.memberId() ) )
            {
                return format( "Database %s switch leader from %s to %s in term %d", dbId, oldLeader.memberId(), newLeader.memberId(), newLeader.term() );
            }
            if ( newLeader.term() != oldLeader.term() )
            {
                return format( "Database %s leader remains %s but term changed to %d", dbId, newLeader.memberId(), newLeader.term() );
            }
            return null;
        } ).filter( Objects::nonNull ).collect( Collectors.joining( newPaddedLine() ) );
    }

    private Set<DatabaseId> getAllDatabases()
    {
        return coreTopologiesByDatabase.keySet();
    }

    private static String newPaddedLine()
    {
        return lineSeparator() + "  ";
    }

    public RaftMemberId resolveRaftMemberForServer( DatabaseId databaseId, ServerId serverId )
    {
        return raftMappingState.resolveRaftMemberForServer( databaseId, serverId );
    }

    public ServerId resolveServerForRaftMember( RaftMemberId memberId )
    {
        return raftMappingState.resolveServerForRaftMember( memberId );
    }

    public Optional<CoreServerInfo> getCoreServerInfo( RaftMemberId raftMemberId )
    {
        // TODO: optimize with single map?
        return ofNullable( raftMappingState.resolveServerForRaftMember( raftMemberId ) )
                .map( id -> coresByServerId.get( id ) );
    }

    public RoleInfo role( NamedDatabaseId namedDatabaseId, ServerId serverId )
    {
        var databaseId = namedDatabaseId.databaseId();
        var rrTopology = readReplicaTopologiesByDatabase.get( databaseId );
        var coreTopology = coreTopologiesByDatabase.get( databaseId );

        if ( coreTopology != null && coreTopology.servers().containsKey( serverId ) )
        {
            var leaderInfo = remoteDbLeaderMap.getOrDefault( databaseId, LeaderInfo.INITIAL );
            var raftMemberId = resolveRaftMemberForServer( namedDatabaseId.databaseId(), serverId );
            if ( Objects.equals( raftMemberId, leaderInfo.memberId() ) )
            {
                return RoleInfo.LEADER;
            }
            return RoleInfo.FOLLOWER;
        }
        else if ( rrTopology != null && rrTopology.servers().containsKey( serverId ) )
        {
            return RoleInfo.READ_REPLICA;
        }
        return RoleInfo.UNKNOWN;
    }

    public Map<ServerId,CoreServerInfo> allCoreServers()
    {
        return coresByServerId;
    }

    public Map<ServerId,ReadReplicaInfo> allReadReplicas()
    {
        return readReplicasByServerId;
    }

    public DatabaseCoreTopology coreTopologyForDatabase( NamedDatabaseId namedDatabaseId )
    {
        var databaseId = namedDatabaseId.databaseId();
        var topology = coreTopologiesByDatabase.get( databaseId );
        return topology != null ? topology : DatabaseCoreTopology.empty( databaseId );
    }

    public DatabaseReadReplicaTopology readReplicaTopologyForDatabase( NamedDatabaseId namedDatabaseId )
    {
        var databaseId = namedDatabaseId.databaseId();
        var topology = readReplicaTopologiesByDatabase.get( databaseId );
        return topology != null ? topology : DatabaseReadReplicaTopology.empty( databaseId );
    }

    public LeaderInfo getLeader( NamedDatabaseId namedDatabaseId )
    {
        return remoteDbLeaderMap.get( namedDatabaseId.databaseId() );
    }

    public void clearRemoteData( ServerId localServerId )
    {
        var coresByServerId = this.coresByServerId;
        var readReplicasByServerId = this.readReplicasByServerId;

        this.coresByServerId = coresByServerId.containsKey( localServerId ) ? Map.of( localServerId, coresByServerId.get( localServerId ) ) : emptyMap();
        this.readReplicasByServerId = readReplicasByServerId.containsKey( localServerId ) ? Map.of( localServerId, readReplicasByServerId.get( localServerId ) )
                                                                                          : emptyMap();
        this.remoteDbLeaderMap = emptyMap();

        bootstrapState = BootstrapState.EMPTY;

        var remoteDataCleaner = new GlobalTopologyCleaner( localServerId );

        this.coreTopologiesByDatabase.replaceAll( remoteDataCleaner::removeRemoteDatabaseCoreTopologies );
        this.coreStatesByDatabase.replaceAll( remoteDataCleaner::removeRemoteReplicatedDatabaseState );

        this.readReplicaTopologiesByDatabase.replaceAll( remoteDataCleaner::removeRemoteDatabaseReadReplicaTopologies );
        this.readReplicaStatesByDatabase.replaceAll( remoteDataCleaner::removeRemoteReplicatedDatabaseState );

        raftMappingState.clearRemoteData( localServerId );
    }

    SocketAddress retrieveCatchupServerAddress( ServerId serverId )
    {
        CoreServerInfo coreServerInfo = coresByServerId.get( serverId );
        if ( coreServerInfo != null )
        {
            SocketAddress address = coreServerInfo.catchupServer();
            log.debug( "Catchup address for core %s was %s", serverId, address );
            return coreServerInfo.catchupServer();
        }
        ReadReplicaInfo readReplicaInfo = readReplicasByServerId.get( serverId );
        if ( readReplicaInfo != null )
        {
            SocketAddress address = readReplicaInfo.catchupServer();
            log.debug( "Catchup address for read replica %s was %s", serverId, address );
            return address;
        }
        log.debug( "Catchup address for member %s not found", serverId );
        return null;
    }

    DiscoveryDatabaseState stateFor( ServerId serverId, NamedDatabaseId namedDatabaseId )
    {
        var databaseId = namedDatabaseId.databaseId();
        return lookupState( serverId, databaseId, coreStatesByDatabase )
                .or( () -> lookupState( serverId, databaseId, readReplicaStatesByDatabase ) )
                .orElse( DiscoveryDatabaseState.unknown( namedDatabaseId.databaseId() ) );
    }

    ReplicatedDatabaseState coreStatesForDatabase( NamedDatabaseId namedDatabaseId )
    {
        var databaseId = namedDatabaseId.databaseId();
        return coreStatesByDatabase.getOrDefault( databaseId, ReplicatedDatabaseState.ofCores( databaseId, Map.of() ) );
    }

    ReplicatedDatabaseState readReplicaStatesForDatabase( NamedDatabaseId namedDatabaseId )
    {
        var databaseId = namedDatabaseId.databaseId();
        return readReplicaStatesByDatabase.getOrDefault( databaseId, ReplicatedDatabaseState.ofReadReplicas( databaseId, Map.of() ) );
    }

    private Optional<DiscoveryDatabaseState> lookupState( ServerId serverId, DatabaseId databaseId,
            Map<DatabaseId,ReplicatedDatabaseState> states )
    {
        return ofNullable( states.get( databaseId ) )
                .flatMap( replicated -> replicated.stateFor( serverId ) );
    }

    BootstrapState bootstrapState()
    {
        return bootstrapState;
    }

    private static <T extends DiscoveryServerInfo> Map<ServerId,T> extractServerInfos( Map<DatabaseId,? extends Topology<T>> topologies )
    {
        Map<ServerId,T> result = new HashMap<>();
        for ( Topology<T> topology : topologies.values() )
        {
            for ( Map.Entry<ServerId,T> entry : topology.servers().entrySet() )
            {
                result.put( entry.getKey(), entry.getValue() );
            }
        }
        return unmodifiableMap( result );
    }

    private static boolean hasNoMembers( Topology<?> topology )
    {
        return topology.servers().isEmpty();
    }
}
