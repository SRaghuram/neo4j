/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaTopology;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.Topology;
import com.neo4j.causalclustering.discovery.TopologyDifference;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;
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
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

public class GlobalTopologyState implements TopologyUpdateSink, DirectoryUpdateSink
{
    private final Log log;
    private final Consumer<CoreTopology> callback;
    private volatile Map<MemberId,AdvertisedSocketAddress> coreCatchupAddressMap;
    private volatile Map<MemberId,AdvertisedSocketAddress> rrCatchupAddressMap;
    private volatile Map<DatabaseId, LeaderInfo> remoteDbLeaderMap;
    private final Map<DatabaseId,CoreTopology> coreTopologiesByDatabase = new ConcurrentHashMap<>();
    private volatile ReadReplicaTopology readReplicaTopology = ReadReplicaTopology.EMPTY;

    public GlobalTopologyState( LogProvider logProvider, Consumer<CoreTopology> listener )
    {
        this.log = logProvider.getLog( getClass() );
        this.coreCatchupAddressMap = emptyMap();
        this.rrCatchupAddressMap = emptyMap();
        this.remoteDbLeaderMap = emptyMap();
        this.callback = listener;
    }

    @Override
    public void onTopologyUpdate( CoreTopology newCoreTopology )
    {
        CoreTopology currentCoreTopology = coreTopologiesByDatabase.put( newCoreTopology.databaseId(), newCoreTopology );
        if ( currentCoreTopology == null )
        {
            currentCoreTopology = CoreTopology.EMPTY;
        }

        TopologyDifference diff = currentCoreTopology.difference( newCoreTopology );
        this.coreCatchupAddressMap = extractCatchupAddressesMap( newCoreTopology );
        if ( diff.hasChanges() )
        {
            log.info( "Core topology changed %s", diff );
            callback.accept( newCoreTopology );
        }
    }

    @Override
    public void onTopologyUpdate( ReadReplicaTopology newReadReplicaTopology )
    {
        TopologyDifference diff = this.readReplicaTopology.difference( newReadReplicaTopology );
        this.readReplicaTopology = newReadReplicaTopology;
        this.rrCatchupAddressMap = extractCatchupAddressesMap( newReadReplicaTopology );
        if ( diff.hasChanges() )
        {
            log.info( "Read replica topology changed %s", diff );
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

    public CoreTopology coreTopologyForDatabase( String databaseName )
    {
        return coreTopologiesByDatabaseName.getOrDefault( databaseName, CoreTopology.EMPTY );
    }

    public CoreTopology coreTopology()
    {
        // todo: do not return default topology like this!
        return coreTopologiesByDatabase.getOrDefault( new DatabaseId( DEFAULT_DATABASE_NAME ), CoreTopology.EMPTY );
    }

    public ReadReplicaTopology readReplicaTopology()
    {
        return readReplicaTopology;
    }

    private Map<MemberId,AdvertisedSocketAddress> extractCatchupAddressesMap( Topology<?> topology )
    {
        return topology.members().entrySet().stream()
                .collect( Collectors.toMap( Map.Entry::getKey, e -> e.getValue().getCatchupServer() ) );
    }

    public AdvertisedSocketAddress retrieveSocketAddress( MemberId memberId )
    {
        AdvertisedSocketAddress coreAddress = coreCatchupAddressMap.get( memberId );
        log.debug( "Catchup address for core %s was %s", memberId, coreAddress );
        return coreAddress != null ? coreAddress : rrCatchupAddressMap.get( memberId );
    }
}
