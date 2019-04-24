/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class TopologyServiceThatPrioritisesItself extends LifecycleAdapter implements TopologyService
{
    private static final DatabaseId DATABASE_ID = new DatabaseId( DEFAULT_DATABASE_NAME );

    private final MemberId memberId;
    private final String matchingGroupName;

    MemberId coreNotSelf = new MemberId( new UUID( 321, 654 ) );
    MemberId readReplicaNotSelf = new MemberId( new UUID( 432, 543 ) );

    TopologyServiceThatPrioritisesItself( MemberId memberId, String matchingGroupName )
    {
        this.memberId = memberId;
        this.matchingGroupName = matchingGroupName;
    }

    @Override
    public Map<MemberId,CoreServerInfo> allCoreServers()
    {
        return Map.of( memberId, coreServerInfo(),
                coreNotSelf, coreServerInfo() );
    }

    @Override
    public DatabaseCoreTopology coreTopologyForDatabase( DatabaseId databaseId )
    {
        return new DatabaseCoreTopology( DATABASE_ID, new ClusterId( new UUID( 99, 88 ) ), true, allCoreServers() );
    }

    @Override
    public Map<MemberId,ReadReplicaInfo> allReadReplicas()
    {
        return Map.of( memberId, readReplicaInfo( matchingGroupName ),
                readReplicaNotSelf, readReplicaInfo( matchingGroupName ) );
    }

    @Override
    public DatabaseReadReplicaTopology readReplicaTopologyForDatabase( DatabaseId databaseId )
    {
        return new DatabaseReadReplicaTopology( DATABASE_ID, allReadReplicas() );
    }

    @Override
    public AdvertisedSocketAddress findCatchupAddress( MemberId upstream )
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public Map<MemberId,RoleInfo> allCoreRoles()
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public MemberId memberId()
    {
        return memberId;
    }

    private static CoreServerInfo coreServerInfo( String... groupNames )
    {
        AdvertisedSocketAddress anyRaftAddress = new AdvertisedSocketAddress( "hostname", 1234 );
        AdvertisedSocketAddress anyCatchupServer = new AdvertisedSocketAddress( "hostname", 5678 );
        ClientConnectorAddresses clientConnectorAddress = new ClientConnectorAddresses( Collections.emptyList() );
        Set<String> groups = Set.of( groupNames );
        Set<DatabaseId> databaseIds = Set.of( DATABASE_ID );
        return new CoreServerInfo( anyRaftAddress, anyCatchupServer, clientConnectorAddress, groups, databaseIds, false );
    }

    private static ReadReplicaInfo readReplicaInfo( String... groupNames )
    {
        ClientConnectorAddresses clientConnectorAddresses = new ClientConnectorAddresses( Collections.emptyList() );
        AdvertisedSocketAddress catchupServerAddress = new AdvertisedSocketAddress( "hostname", 2468 );
        Set<String> groups = Set.of( groupNames );
        Set<DatabaseId> databaseIds = Set.of( DATABASE_ID );
        return new ReadReplicaInfo( clientConnectorAddresses, catchupServerAddress, groups, databaseIds );
    }
}
