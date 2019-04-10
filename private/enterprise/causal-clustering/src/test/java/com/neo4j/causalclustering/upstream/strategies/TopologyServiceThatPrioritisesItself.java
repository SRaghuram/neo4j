/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.ReadReplicaTopology;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class TopologyServiceThatPrioritisesItself extends LifecycleAdapter implements TopologyService
{
    private static final String DATABASE_NAME = DEFAULT_DATABASE_NAME;

    private final MemberId myself;
    private final String matchingGroupName;

    MemberId coreNotSelf = new MemberId( new UUID( 321, 654 ) );
    MemberId readReplicaNotSelf = new MemberId( new UUID( 432, 543 ) );

    TopologyServiceThatPrioritisesItself( MemberId myself, String matchingGroupName )
    {
        this.myself = myself;
        this.matchingGroupName = matchingGroupName;
    }

    @Override
    public DatabaseId localDatabaseId()
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public CoreTopology allCoreServers()
    {
        boolean canBeBootstrapped = true;
        Map<MemberId,CoreServerInfo> coreMembers = new HashMap<>();
        coreMembers.put( myself, coreServerInfo() );
        coreMembers.put( coreNotSelf, coreServerInfo() );
        return new CoreTopology( new DatabaseId( DATABASE_NAME ), new ClusterId( new UUID( 99, 88 ) ), canBeBootstrapped, coreMembers );
    }

    @Override
    public ReadReplicaTopology allReadReplicas()
    {
        Map<MemberId,ReadReplicaInfo> readReplicaMembers = new HashMap<>();
        readReplicaMembers.put( myself, readReplicaInfo( matchingGroupName ) );
        readReplicaMembers.put( readReplicaNotSelf, readReplicaInfo( matchingGroupName ) );
        return new ReadReplicaTopology( DATABASE_NAME, readReplicaMembers );
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
    public MemberId myself()
    {
        return myself;
    }

    private static CoreServerInfo coreServerInfo( String... groupNames )
    {
        AdvertisedSocketAddress anyRaftAddress = new AdvertisedSocketAddress( "hostname", 1234 );
        AdvertisedSocketAddress anyCatchupServer = new AdvertisedSocketAddress( "hostname", 5678 );
        ClientConnectorAddresses clientConnectorAddress = new ClientConnectorAddresses( Collections.emptyList() );
        Set<String> groups = Set.of( groupNames );
        Set<DatabaseId> databaseIds = Set.of( new DatabaseId( DATABASE_NAME ) );
        return new CoreServerInfo( anyRaftAddress, anyCatchupServer, clientConnectorAddress, groups, databaseIds, false );
    }

    private static ReadReplicaInfo readReplicaInfo( String... groupNames )
    {
        ClientConnectorAddresses clientConnectorAddresses = new ClientConnectorAddresses( Collections.emptyList() );
        AdvertisedSocketAddress catchupServerAddress = new AdvertisedSocketAddress( "hostname", 2468 );
        Set<String> groups = Set.of( groupNames );
        Set<DatabaseId> databaseIds = Set.of( new DatabaseId( DATABASE_NAME ) );
        return new ReadReplicaInfo( clientConnectorAddresses, catchupServerAddress, groups, databaseIds );
    }
}
