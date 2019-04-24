/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

class FakeTopologyService extends LifecycleAdapter implements TopologyService
{
    private final ClusterId clusterId;
    private final Map<MemberId,CoreServerInfo> coreMembers;
    private final Map<MemberId,RoleInfo> roles;
    private final Map<MemberId,ReadReplicaInfo> replicaMembers;
    private final MemberId myself;

    FakeTopologyService( Collection<MemberId> cores, Collection<MemberId> replicas, MemberId myself, RoleInfo myselfRole )
    {
        this.myself = myself;
        clusterId = new ClusterId( UUID.randomUUID() );
        roles = new HashMap<>();
        coreMembers = new HashMap<>();

        for ( MemberId coreMemberId : cores )
        {
            CoreServerInfo coreServerInfo = coreServerInfo();
            coreMembers.put( coreMemberId, coreServerInfo );
            roles.put( coreMemberId, RoleInfo.FOLLOWER );
        }

        replicaMembers = new HashMap<>();
        for ( MemberId replicaMemberId : replicas )
        {
            ReadReplicaInfo readReplicaInfo = readReplicaInfo();
            replicaMembers.put( replicaMemberId, readReplicaInfo );
            roles.put( replicaMemberId, RoleInfo.READ_REPLICA );
        }

        if ( RoleInfo.READ_REPLICA.equals( myselfRole ) )
        {
            replicaMembers.put( myself, readReplicaInfo() );
            roles.put( myself, RoleInfo.READ_REPLICA );
        }
        else
        {
            coreMembers.put( myself, coreServerInfo() );
            roles.put( myself, RoleInfo.FOLLOWER );
        }
        roles.put( myself, myselfRole );
    }

    private static CoreServerInfo coreServerInfo()
    {
        AdvertisedSocketAddress raftServer = new AdvertisedSocketAddress( "hostname", 1234 );
        AdvertisedSocketAddress catchupServer = new AdvertisedSocketAddress( "hostname", 1234 );
        ClientConnectorAddresses clientConnectors = new ClientConnectorAddresses( Collections.emptyList() );
        Set<String> groups = Set.of();
        Set<DatabaseId> databaseIds = Set.of( new DatabaseId( DEFAULT_DATABASE_NAME ) );
        boolean refuseToBeLeader = false;
        return new CoreServerInfo( raftServer, catchupServer, clientConnectors, groups, databaseIds, refuseToBeLeader );
    }

    private static ReadReplicaInfo readReplicaInfo()
    {
        ClientConnectorAddresses clientConnectorAddresses = new ClientConnectorAddresses( List.of() );
        AdvertisedSocketAddress catchupServerAddress = new AdvertisedSocketAddress( "hostname", 1234 );
        Set<String> groups = Set.of();
        Set<DatabaseId> databaseIds = Set.of( new DatabaseId( DEFAULT_DATABASE_NAME ) );
        return new ReadReplicaInfo( clientConnectorAddresses, catchupServerAddress, groups, databaseIds );
    }

    @Override
    public Map<MemberId,CoreServerInfo> allCoreServers()
    {
        return coreMembers;
    }

    @Override
    public DatabaseCoreTopology coreTopologyForDatabase( DatabaseId databaseId )
    {
        return new DatabaseCoreTopology( databaseId, clusterId, true, coreMembers );
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
    public AdvertisedSocketAddress findCatchupAddress( MemberId upstream ) throws CatchupAddressResolutionException
    {
        throw new CatchupAddressResolutionException( upstream );
    }

    @Override
    public Map<MemberId,RoleInfo> allCoreRoles()
    {
        Map<MemberId,RoleInfo> roles = new HashMap<>();
        for ( MemberId memberId : coreMembers.keySet() )
        {
            roles.put( memberId, this.roles.get( memberId ) );
        }
        return roles;
    }

    @Override
    public MemberId myself()
    {
        return myself;
    }

    public void replaceWithRole( MemberId memberId, RoleInfo role )
    {
        List<MemberId> membersWithRole = roles.keySet().stream().filter( member -> roles.get( member ).equals( role ) ).collect( Collectors.toList());
        if ( membersWithRole.size() == 1 )
        {
            roles.put( membersWithRole.get( 0 ), RoleInfo.FOLLOWER );
        }
        if ( memberId != null )
        {
            roles.put( memberId, role );
        }
    }
}
