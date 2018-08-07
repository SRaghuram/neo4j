/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.rest.causalclustering;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.neo4j.causalclustering.discovery.ClientConnectorAddresses;
import org.neo4j.causalclustering.discovery.CoreServerInfo;
import org.neo4j.causalclustering.discovery.CoreTopology;
import org.neo4j.causalclustering.discovery.ReadReplicaInfo;
import org.neo4j.causalclustering.discovery.ReadReplicaTopology;
import org.neo4j.causalclustering.discovery.RoleInfo;
import org.neo4j.causalclustering.discovery.TopologyService;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.helpers.AdvertisedSocketAddress;

class FakeTopologyService implements TopologyService
{
    private final ClusterId clusterId;
    private final Map<MemberId,CoreServerInfo> coreMembers;
    private final Map<MemberId,RoleInfo> roles;
    private final Map<MemberId,ReadReplicaInfo> replicaMembers;
    private final String dbName = "dbName";
    private final MemberId myself;

    FakeTopologyService( int numCores, int numReplicas, MemberId myself, RoleInfo myselfRole )
    {
        this.myself = myself;
        clusterId = new ClusterId( UUID.randomUUID() );
        roles = new HashMap<>();
        coreMembers = new HashMap<>();

        for ( int i = 0; i < numCores; i++ )
        {
            CoreServerInfo coreServerInfo = coreServerInfo( dbName );
            MemberId memberId = new MemberId( UUID.randomUUID() );
            coreMembers.put( memberId, coreServerInfo );
            roles.put( memberId, RoleInfo.FOLLOWER );
        }

        replicaMembers = new HashMap<>();
        for ( int i = 0; i < numReplicas; i++ )
        {
            ReadReplicaInfo readReplicaInfo = readReplicaInfo( dbName );
            MemberId memberId = new MemberId( UUID.randomUUID() );
            replicaMembers.put( memberId, readReplicaInfo );
            roles.put( memberId, RoleInfo.READ_REPLICA );
        }

        if ( RoleInfo.READ_REPLICA.equals( myselfRole ) )
        {
            replicaMembers.put( myself, readReplicaInfo( dbName ) );
        }
        else
        {
            coreMembers.put( myself, coreServerInfo( dbName ) );
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
    public Optional<AdvertisedSocketAddress> findCatchupAddress( MemberId upstream )
    {
        return Optional.empty();
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

    public void makeLeader( MemberId memberId )
    {
        Optional<MemberId> leader = roles.keySet().stream().filter( member -> roles.get( member ).equals( RoleInfo.LEADER ) ).findFirst();
        if ( leader.isPresent() )
        {
            roles.put( leader.get(), RoleInfo.FOLLOWER );
        }
        if ( memberId != null )
        {
            roles.put( memberId, RoleInfo.LEADER );
        }
    }
}
