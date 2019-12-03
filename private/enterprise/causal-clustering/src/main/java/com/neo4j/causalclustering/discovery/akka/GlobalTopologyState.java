/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.ReplicatedDatabaseState;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.Topology;
import com.neo4j.causalclustering.discovery.akka.coretopology.BootstrapState;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static java.lang.System.lineSeparator;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static org.neo4j.internal.helpers.Strings.printMap;

public class GlobalTopologyState implements TopologyUpdateSink, DirectoryUpdateSink, BootstrapStateUpdateSink, DatabaseStateUpdateSink
{
    private final Log log;
    private final Consumer<DatabaseCoreTopology> callback;
    private volatile Map<MemberId,CoreServerInfo> coresByMemberId;
    private volatile Map<MemberId,ReadReplicaInfo> readReplicasByMemberId;
    private volatile Map<DatabaseId,LeaderInfo> remoteDbLeaderMap;
    private volatile BootstrapState bootstrapState = BootstrapState.EMPTY;
    private final Map<DatabaseId,DatabaseCoreTopology> coreTopologiesByDatabase = new ConcurrentHashMap<>();
    private final Map<DatabaseId,DatabaseReadReplicaTopology> readReplicaTopologiesByDatabase = new ConcurrentHashMap<>();
    private final Map<DatabaseId, ReplicatedDatabaseState> coreStatesByDatabase = new ConcurrentHashMap<>();
    private final Map<DatabaseId, ReplicatedDatabaseState> readReplicaStatesByDatabase = new ConcurrentHashMap<>();

    GlobalTopologyState( LogProvider logProvider, Consumer<DatabaseCoreTopology> listener )
    {
        this.log = logProvider.getLog( getClass() );
        this.coresByMemberId = emptyMap();
        this.readReplicasByMemberId = emptyMap();
        this.remoteDbLeaderMap = emptyMap();
        this.callback = listener;
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
            this.coresByMemberId = extractServerInfos( coreTopologiesByDatabase );
            logTopologyChange( "Core topology", newCoreTopology, databaseId, currentCoreTopology );
            callback.accept( newCoreTopology );
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
            this.readReplicasByMemberId = extractServerInfos( readReplicaTopologiesByDatabase );
            logTopologyChange( "Read replica topology", currentReadReplicaTopology, databaseId, newReadReplicaTopology );
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
            log.info( "Database leader(s) update:%s", newPaddedLine(), printLeaderInfoMap( leaderInfoMap, remoteDbLeaderMap ) );
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
            StringBuilder stringBuilder = new StringBuilder( format( "The %s replicated states for database %s changed", role, databaseId ) );
            if ( previousState == null )
            {
                stringBuilder.append( " previous state was empty" );
            }
            else
            {
                stringBuilder.append( "previous state was:" ).append( newPaddedLine() ).append( printMap( previousState.memberStates(), newPaddedLine() ) )
                        .append( lineSeparator() );
            }
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

    private static String printLeaderInfoMap( Map<DatabaseId,LeaderInfo> leaderInfoMap, Map<DatabaseId,LeaderInfo> oldDbLeaderMap )
    {
        return leaderInfoMap.entrySet().stream().map( entry ->
        {
            if ( !oldDbLeaderMap.containsKey( entry.getKey() ) )
            {
                return format( "Discovered database %s with leader %s in term %d", entry.getKey(), entry.getValue().memberId(), entry.getValue().term() );
            }
            else if ( entry.getValue().memberId().equals( oldDbLeaderMap.get( entry.getKey() ).memberId() ) )
            {
                return format( "Database %s has leader %s in term %d", entry.getKey(), entry.getValue().memberId(), entry.getValue().term() );
            }
            else
            {
                return format( "Database %s switch leader from %s to %s in term %d", entry.getKey(), oldDbLeaderMap.get( entry.getKey() ).memberId(),
                        entry.getValue().memberId(), entry.getValue().term() );
            }
        } ).collect( Collectors.joining( newPaddedLine() ) );
    }

    private static String newPaddedLine()
    {
        return lineSeparator() + "  ";
    }

    private void logTopologyChange( String topologyDescription, Topology<?> newTopology, DatabaseId databaseId, Topology<?> oldTopology )
    {
        var allMembers = Collections.unmodifiableSet( newTopology.members().keySet() );

        var lostMembers = new HashSet<>( oldTopology.members().keySet() );
        lostMembers.removeAll( allMembers );

        var newMembers = new HashMap<>( newTopology.members() );
        newMembers.keySet().removeAll( oldTopology.members().keySet() );

        String logLine =
                format( "%s for database %s is now: %s", topologyDescription, databaseId, allMembers.isEmpty() ? "empty" : allMembers )
                        + lineSeparator() +
                        (lostMembers.isEmpty() ? "No members where lost" : format( "Lost members :%s", lostMembers ))
                        + lineSeparator() +
                        (newMembers.isEmpty() ? "No new members" : format( "New members: %s%s", newPaddedLine(), printMap( newMembers, newPaddedLine() ) ));
        log.info( logLine );
    }

    public RoleInfo role( NamedDatabaseId namedDatabaseId, MemberId memberId )
    {
        var databaseId = namedDatabaseId.databaseId();
        var rrTopology = readReplicaTopologiesByDatabase.get( databaseId );
        var coreTopology = coreTopologiesByDatabase.get( databaseId );

        if ( coreTopology != null && coreTopology.members().containsKey( memberId ) )
        {
            var leaderInfo = remoteDbLeaderMap.getOrDefault( databaseId, LeaderInfo.INITIAL );
            if ( Objects.equals( memberId, leaderInfo.memberId() ) )
            {
                return RoleInfo.LEADER;
            }
            return RoleInfo.FOLLOWER;
        }
        else if ( rrTopology != null && rrTopology.members().containsKey( memberId ) )
        {
            return RoleInfo.READ_REPLICA;
        }
        return RoleInfo.UNKNOWN;
    }

    public Map<MemberId,CoreServerInfo> allCoreServers()
    {
        return coresByMemberId;
    }

    public Map<MemberId,ReadReplicaInfo> allReadReplicas()
    {
        return readReplicasByMemberId;
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

    SocketAddress retrieveCatchupServerAddress( MemberId memberId )
    {
        CoreServerInfo coreServerInfo = coresByMemberId.get( memberId );
        if ( coreServerInfo != null )
        {
            SocketAddress address = coreServerInfo.catchupServer();
            log.debug( "Catchup address for core %s was %s", memberId, address );
            return coreServerInfo.catchupServer();
        }
        ReadReplicaInfo readReplicaInfo = readReplicasByMemberId.get( memberId );
        if ( readReplicaInfo != null )
        {
            SocketAddress address = readReplicaInfo.catchupServer();
            log.debug( "Catchup address for read replica %s was %s", memberId, address );
            return address;
        }
        log.debug( "Catchup address for member %s not found", memberId );
        return null;
    }

    DiscoveryDatabaseState stateFor( MemberId memberId, NamedDatabaseId namedDatabaseId )
    {
        var databaseId = namedDatabaseId.databaseId();
        return lookupState( memberId, databaseId, coreStatesByDatabase )
                .or( () -> lookupState( memberId, databaseId, readReplicaStatesByDatabase ) )
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

    private Optional<DiscoveryDatabaseState> lookupState( MemberId memberId, DatabaseId databaseId,
            Map<DatabaseId,ReplicatedDatabaseState> states )
    {
        return Optional.ofNullable( states.get( databaseId ) )
                .flatMap( replicated -> replicated.stateFor( memberId ) );
    }

    BootstrapState bootstrapState()
    {
        return bootstrapState;
    }

    private static <T extends DiscoveryServerInfo> Map<MemberId,T> extractServerInfos( Map<DatabaseId,? extends Topology<T>> topologies )
    {
        Map<MemberId,T> result = new HashMap<>();
        for ( Topology<T> topology : topologies.values() )
        {
            for ( Map.Entry<MemberId,T> entry : topology.members().entrySet() )
            {
                result.put( entry.getKey(), entry.getValue() );
            }
        }
        return unmodifiableMap( result );
    }

    private static boolean hasNoMembers( Topology<?> topology )
    {
        return topology.members().isEmpty();
    }
}
