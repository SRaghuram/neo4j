/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.upstream.strategies;

import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.ConnectorAddresses;
import com.neo4j.causalclustering.discovery.CoreServerInfo;
import com.neo4j.causalclustering.discovery.DatabaseCoreTopology;
import com.neo4j.causalclustering.discovery.DatabaseReadReplicaTopology;
import com.neo4j.causalclustering.discovery.ReadReplicaInfo;
import com.neo4j.causalclustering.discovery.RoleInfo;
import com.neo4j.causalclustering.discovery.TopologyService;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.identity.RaftGroupId;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.configuration.ServerGroupName;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.DatabaseState;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

class TopologyServiceThatPrioritisesItself extends LifecycleAdapter implements TopologyService
{
    private static final DatabaseId DATABASE_ID = new TestDatabaseIdRepository().defaultDatabase().databaseId();

    private final ServerId serverId;
    private final ServerGroupName matchingGroupName;

    private final ServerId coreNotSelf = new ServerId( new UUID( 321, 654 ) );
    private final ServerId readReplicaNotSelf = new ServerId( new UUID( 432, 543 ) );

    TopologyServiceThatPrioritisesItself( ServerId serverId, ServerGroupName matchingGroupName )
    {
        this.serverId = serverId;
        this.matchingGroupName = matchingGroupName;
    }

    @Override
    public void onDatabaseStart( NamedDatabaseId namedDatabaseId )
    {
    }

    @Override
    public void onRaftMemberKnown( NamedDatabaseId namedDatabaseId )
    {
    }

    @Override
    public void onDatabaseStop( NamedDatabaseId namedDatabaseId )
    {
    }

    @Override
    public void stateChange( DatabaseState previousState, DatabaseState newState )
    {
    }

    @Override
    public Map<ServerId,CoreServerInfo> allCoreServers()
    {
        return Map.of( serverId, coreServerInfo(),
                coreNotSelf, coreServerInfo() );
    }

    @Override
    public DatabaseCoreTopology coreTopologyForDatabase( NamedDatabaseId namedDatabaseId )
    {
        var databaseId = namedDatabaseId.databaseId();
        return new DatabaseCoreTopology( databaseId, RaftGroupId.from( databaseId ), allCoreServers() );
    }

    @Override
    public Map<ServerId,ReadReplicaInfo> allReadReplicas()
    {
        return Map.of( serverId, readReplicaInfo( matchingGroupName ),
                readReplicaNotSelf, readReplicaInfo( matchingGroupName ) );
    }

    @Override
    public DatabaseReadReplicaTopology readReplicaTopologyForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return new DatabaseReadReplicaTopology( namedDatabaseId.databaseId(), allReadReplicas() );
    }

    @Override
    public SocketAddress lookupCatchupAddress( ServerId upstream )
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public RoleInfo lookupRole( NamedDatabaseId namedDatabaseId, ServerId serverId )
    {
        return RoleInfo.UNKNOWN;
    }

    @Override
    public LeaderInfo getLeader( NamedDatabaseId namedDatabaseId )
    {
        return null;
    }

    @Override
    public ServerId serverId()
    {
        return serverId;
    }

    @Override
    public RaftMemberId resolveRaftMemberForServer( NamedDatabaseId namedDatabaseId, ServerId serverId )
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public ServerId resolveServerForRaftMember( RaftMemberId raftMemberId )
    {
        throw new RuntimeException( "Unimplemented" );
    }

    @Override
    public DiscoveryDatabaseState lookupDatabaseState( NamedDatabaseId namedDatabaseId, ServerId serverId )
    {
        return DiscoveryDatabaseState.unknown( namedDatabaseId.databaseId() );
    }

    @Override
    public Map<ServerId,DiscoveryDatabaseState> allCoreStatesForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return Map.of();
    }

    @Override
    public Map<ServerId,DiscoveryDatabaseState> allReadReplicaStatesForDatabase( NamedDatabaseId namedDatabaseId )
    {
        return Map.of();
    }

    @Override
    public boolean isHealthy()
    {
        return true;
    }

    private static CoreServerInfo coreServerInfo( ServerGroupName... groupNames )
    {
        SocketAddress anyRaftAddress = new SocketAddress( "hostname", 1234 );
        SocketAddress anyCatchupServer = new SocketAddress( "hostname", 5678 );
        ConnectorAddresses clientConnectorAddress = ConnectorAddresses.fromList( Collections.emptyList() );
        Set<ServerGroupName> groups = Set.of( groupNames );
        Set<DatabaseId> databaseIds = Set.of( DATABASE_ID );
        return new CoreServerInfo( anyRaftAddress, anyCatchupServer, clientConnectorAddress, groups, databaseIds );
    }

    private static ReadReplicaInfo readReplicaInfo( ServerGroupName... groupNames )
    {
        ConnectorAddresses connectorAddresses = ConnectorAddresses.fromList( Collections.emptyList() );
        SocketAddress catchupServerAddress = new SocketAddress( "hostname", 2468 );
        Set<ServerGroupName> groups = Set.of( groupNames );
        Set<DatabaseId> databaseIds = Set.of( DATABASE_ID );
        return new ReadReplicaInfo( connectorAddresses, catchupServerAddress, groups, databaseIds );
    }
}
