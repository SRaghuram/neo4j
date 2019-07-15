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
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.Topology;
import com.neo4j.causalclustering.discovery.akka.coretopology.BootstrapState;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class GlobalTopologyState implements TopologyUpdateSink, DirectoryUpdateSink, BootstrapStateUpdateSink
{
    private final Log log;
    private final Consumer<DatabaseCoreTopology> callback;
    private volatile Map<MemberId,CoreServerInfo> coresByMemberId;
    private volatile Map<MemberId,ReadReplicaInfo> readReplicasByMemberId;
    private volatile Map<DatabaseId, LeaderInfo> remoteDbLeaderMap;
    private volatile BootstrapState bootstrapState = BootstrapState.EMPTY;
    private final Map<DatabaseId,DatabaseCoreTopology> coreTopologiesByDatabase = new ConcurrentHashMap<>();
    private final Map<DatabaseId,DatabaseReadReplicaTopology> readReplicaTopologiesByDatabase = new ConcurrentHashMap<>();

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
        DatabaseId databaseId = newCoreTopology.databaseId();
        DatabaseCoreTopology currentCoreTopology = coreTopologiesByDatabase.put( databaseId, newCoreTopology );

        if ( !Objects.equals( currentCoreTopology, newCoreTopology ) )
        {
            this.coresByMemberId = extractServerInfos( coreTopologiesByDatabase );
            log.info( "Core topology for database %s changed from %s to %s", databaseId, currentCoreTopology, newCoreTopology );
            callback.accept( newCoreTopology );
        }

        if ( hasNoMembers( newCoreTopology ) )
        {
            // do not store core topologies with no members, they can represent deleted databases
            coreTopologiesByDatabase.remove( databaseId, newCoreTopology );
        }
    }

    @Override
    public void onTopologyUpdate( DatabaseReadReplicaTopology newReadReplicaTopology )
    {
        DatabaseId databaseId = newReadReplicaTopology.databaseId();
        DatabaseReadReplicaTopology currentReadReplicaTopology = readReplicaTopologiesByDatabase.put( databaseId, newReadReplicaTopology );

        if ( !Objects.equals( currentReadReplicaTopology, newReadReplicaTopology ) )
        {
            this.readReplicasByMemberId = extractServerInfos( readReplicaTopologiesByDatabase );
            log.info( "Read replica topology for database %s changed from %s to %s", databaseId, currentReadReplicaTopology, newReadReplicaTopology );
        }

        if ( hasNoMembers( newReadReplicaTopology ) )
        {
            // do not store read replica topologies with no members, they can represent deleted databases
            readReplicaTopologiesByDatabase.remove( databaseId, newReadReplicaTopology );
        }
    }

    @Override
    public void onDbLeaderUpdate( Map<DatabaseId, LeaderInfo> leaderInfoMap )
    {
        if ( !leaderInfoMap.equals( remoteDbLeaderMap ) )
        {
            log.info( "Database leaders changed: %s", leaderInfoMap );
            this.remoteDbLeaderMap = leaderInfoMap;
        }
    }

    @Override
    public void onBootstrapStateUpdate( BootstrapState newBootstrapState )
    {
        bootstrapState = requireNonNull( newBootstrapState );
    }

    public RoleInfo coreRole( DatabaseId databaseId, MemberId memberId )
    {
        var coreTopology = coreTopologiesByDatabase.get( databaseId );
        if ( coreTopology == null || !coreTopology.members().containsKey( memberId ) )
        {
            return RoleInfo.UNKNOWN;
        }
        var leaderInfo = remoteDbLeaderMap.getOrDefault( databaseId, LeaderInfo.INITIAL );
        if ( Objects.equals( memberId, leaderInfo.memberId() ) )
        {
            return RoleInfo.LEADER;
        }
        return RoleInfo.FOLLOWER;
    }

    public Map<MemberId,CoreServerInfo> allCoreServers()
    {
        return coresByMemberId;
    }

    public Map<MemberId,ReadReplicaInfo> allReadReplicas()
    {
        return readReplicasByMemberId;
    }

    public DatabaseCoreTopology coreTopologyForDatabase( DatabaseId databaseId )
    {
        return coreTopologiesByDatabase.getOrDefault( databaseId, DatabaseCoreTopology.EMPTY );
    }

    public DatabaseReadReplicaTopology readReplicaTopologyForDatabase( DatabaseId databaseId )
    {
        return readReplicaTopologiesByDatabase.getOrDefault( databaseId, DatabaseReadReplicaTopology.EMPTY );
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
