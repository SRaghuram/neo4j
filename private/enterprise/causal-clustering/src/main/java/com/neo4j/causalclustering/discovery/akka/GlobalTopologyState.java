/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.DiscoveryServerInfo;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.ReadReplicaTopology;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.Topology;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

public class GlobalTopologyState implements TopologyUpdateSink, DirectoryUpdateSink
{
    private final Log log;
    private final Consumer<CoreTopology> callback;
    private volatile Map<MemberId,CoreServerInfo> coresByMemberId;
    private volatile Map<MemberId,ReadReplicaInfo> readReplicasByMemberId;
    private volatile Map<DatabaseId, LeaderInfo> remoteDbLeaderMap;
    private final Map<DatabaseId,CoreTopology> coreTopologiesByDatabase = new ConcurrentHashMap<>();
    private final Map<DatabaseId,ReadReplicaTopology> readReplicaTopologiesByDatabase = new ConcurrentHashMap<>();

    public GlobalTopologyState( LogProvider logProvider, Consumer<CoreTopology> listener )
    {
        this.log = logProvider.getLog( getClass() );
        this.coresByMemberId = emptyMap();
        this.readReplicasByMemberId = emptyMap();
        this.remoteDbLeaderMap = emptyMap();
        this.callback = listener;
    }

    @Override
    public void onTopologyUpdate( CoreTopology newCoreTopology )
    {
        DatabaseId databaseId = newCoreTopology.databaseId();
        CoreTopology currentCoreTopology = coreTopologiesByDatabase.put( databaseId, newCoreTopology );

        if ( haveDifferentMembers( currentCoreTopology, newCoreTopology ) )
        {
            this.coresByMemberId = extractServerInfos( coreTopologiesByDatabase );
            log.info( "Core topology for database %s changed from %s to %s", databaseId, currentCoreTopology, newCoreTopology );
            callback.accept( newCoreTopology );
        }
    }

    @Override
    public void onTopologyUpdate( ReadReplicaTopology newReadReplicaTopology )
    {
        DatabaseId databaseId = newReadReplicaTopology.databaseId();
        ReadReplicaTopology currentReadReplicaTopology = readReplicaTopologiesByDatabase.put( databaseId, newReadReplicaTopology );

        if ( !Objects.equals( currentReadReplicaTopology, newReadReplicaTopology ) )
        {
            this.readReplicasByMemberId = extractServerInfos( readReplicaTopologiesByDatabase );
            log.info( "Read replica topology for database %s changed from %s to %s", databaseId, currentReadReplicaTopology, newReadReplicaTopology );
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

    public Map<MemberId,RoleInfo> allCoreRoles()
    {
        Set<MemberId> leaders = remoteDbLeaderMap.values().stream().map( LeaderInfo::memberId ).collect( Collectors.toSet() );
        Set<MemberId> allCoreMembers = coreTopologiesByDatabase.values().stream()
                .flatMap( topology -> topology.members().keySet().stream() )
                .collect( Collectors.toSet() );

        Function<MemberId,RoleInfo> roleMapper = m -> leaders.contains( m ) ? RoleInfo.LEADER : RoleInfo.FOLLOWER;
        return allCoreMembers.stream().collect( Collectors.toMap( Function.identity(), roleMapper ) );
    }

    public Map<MemberId,CoreServerInfo> allCoreServers()
    {
        return coresByMemberId;
    }

    public Map<MemberId,ReadReplicaInfo> allReadReplicas()
    {
        return readReplicasByMemberId;
    }

    public CoreTopology coreTopologyForDatabase( DatabaseId databaseId )
    {
        return coreTopologiesByDatabase.getOrDefault( databaseId, CoreTopology.EMPTY );
    }

    public ReadReplicaTopology readReplicaTopologyForDatabase( DatabaseId databaseId )
    {
        return readReplicaTopologiesByDatabase.getOrDefault( databaseId, ReadReplicaTopology.EMPTY );
    }

    public AdvertisedSocketAddress retrieveCatchupServerAddress( MemberId memberId )
    {
        CoreServerInfo coreServerInfo = coresByMemberId.get( memberId );
        if ( coreServerInfo != null )
        {
            AdvertisedSocketAddress address = coreServerInfo.getCatchupServer();
            log.debug( "Catchup address for core %s was %s", memberId, address );
            return coreServerInfo.getCatchupServer();
        }
        ReadReplicaInfo readReplicaInfo = readReplicasByMemberId.get( memberId );
        if ( readReplicaInfo != null )
        {
            AdvertisedSocketAddress address = readReplicaInfo.getCatchupServer();
            log.debug( "Catchup address for read replica %s was %s", memberId, address );
            return address;
        }
        log.debug( "Catchup address for member %s not found", memberId );
        return null;
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

    private static boolean haveDifferentMembers( CoreTopology oldTopology, CoreTopology newTopology )
    {
        return oldTopology == null || !oldTopology.members().equals( newTopology.members() );
    }
}
