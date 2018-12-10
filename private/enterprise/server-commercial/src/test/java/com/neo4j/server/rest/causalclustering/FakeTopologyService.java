/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.CoreTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.ReadReplicaTopology;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;

class FakeTopologyService implements TopologyService
{
    private final ClusterId clusterId;
    private final Map<MemberId,CoreServerInfo> coreMembers;
    private final Map<MemberId,RoleInfo> roles;
    private final Map<MemberId,ReadReplicaInfo> replicaMembers;
    private final String dbName = "dbName";
    private final MemberId myself;

    FakeTopologyService( Collection<MemberId> cores, Collection<MemberId> replicas, MemberId myself, RoleInfo myselfRole )
    {
        this.myself = myself;
        clusterId = new ClusterId( UUID.randomUUID() );
        roles = new HashMap<>();
        coreMembers = new HashMap<>();

        for ( MemberId coreMemberId : cores )
        {
            CoreServerInfo coreServerInfo = coreServerInfo( dbName );
            coreMembers.put( coreMemberId, coreServerInfo );
            roles.put( coreMemberId, RoleInfo.FOLLOWER );
        }

        replicaMembers = new HashMap<>();
        for ( MemberId replicaMemberId : replicas )
        {
            ReadReplicaInfo readReplicaInfo = readReplicaInfo( dbName );
            replicaMembers.put( replicaMemberId, readReplicaInfo );
            roles.put( replicaMemberId, RoleInfo.READ_REPLICA );
        }

        if ( RoleInfo.READ_REPLICA.equals( myselfRole ) )
        {
            replicaMembers.put( myself, readReplicaInfo( dbName ) );
            roles.put( myself, RoleInfo.READ_REPLICA );
        }
        else
        {
            coreMembers.put( myself, coreServerInfo( dbName ) );
            roles.put( myself, RoleInfo.FOLLOWER );
        }
        roles.put( myself, myselfRole );
    }

    private CoreServerInfo coreServerInfo( String dbName )
    {
        AdvertisedSocketAddress raftServer = new AdvertisedSocketAddress( "hostname", 1234 );
        AdvertisedSocketAddress catchupServer = new AdvertisedSocketAddress( "hostname", 1234 );
        ClientConnectorAddresses clientConnectors = new ClientConnectorAddresses( Collections.emptyList() );
        boolean refuseToBeLeader = false;
        return new CoreServerInfo( raftServer, catchupServer, clientConnectors, dbName, refuseToBeLeader );
    }

    private ReadReplicaInfo readReplicaInfo( String dbName )
    {
        ClientConnectorAddresses clientConnectorAddresses = new ClientConnectorAddresses( Collections.emptyList() );
        AdvertisedSocketAddress catchupServerAddress = new AdvertisedSocketAddress( "hostname", 1234 );
        return new ReadReplicaInfo( clientConnectorAddresses, catchupServerAddress, dbName );
    }

    @Override
    public String localDBName()
    {
        return dbName;
    }

    @Override
    public CoreTopology allCoreServers()
    {
        return new CoreTopology( clusterId, true, coreMembers );
    }

    @Override
    public CoreTopology localCoreServers()
    {
        return new CoreTopology( clusterId, true, coreMembers );
    }

    @Override
    public ReadReplicaTopology allReadReplicas()
    {
        return new ReadReplicaTopology( replicaMembers );
    }

    @Override
    public ReadReplicaTopology localReadReplicas()
    {
        return new ReadReplicaTopology( replicaMembers );
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

    @Override
    public void init() throws Throwable
    {

    }

    @Override
    public void start() throws Throwable
    {

    }

    @Override
    public void stop() throws Throwable
    {

    }

    @Override
    public void shutdown() throws Throwable
    {

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
