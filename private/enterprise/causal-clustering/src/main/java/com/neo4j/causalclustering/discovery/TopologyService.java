/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.dbms.DatabaseStateChangedListener;

import java.util.Map;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.identity.ServerId;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Provides a read-only service for the eventually consistent topology information.
 */
public interface TopologyService extends Lifecycle, DatabaseStateChangedListener
{
    void onDatabaseStart( NamedDatabaseId namedDatabaseId );

    void onRaftMemberKnown( NamedDatabaseId namedDatabaseId );

    void onDatabaseStop( NamedDatabaseId namedDatabaseId );

    ServerId serverId();

    Map<ServerId,CoreServerInfo> allCoreServers();

    Map<ServerId,ReadReplicaInfo> allReadReplicas();

    DatabaseCoreTopology coreTopologyForDatabase( NamedDatabaseId namedDatabaseId );

    DatabaseReadReplicaTopology readReplicaTopologyForDatabase( NamedDatabaseId namedDatabaseId );

    SocketAddress lookupCatchupAddress( ServerId upstream ) throws CatchupAddressResolutionException;

    LeaderInfo getLeader( NamedDatabaseId namedDatabaseId );

    RoleInfo lookupRole( NamedDatabaseId namedDatabaseId, ServerId serverId );

    DiscoveryDatabaseState lookupDatabaseState( NamedDatabaseId namedDatabaseId, ServerId serverId );

    Map<ServerId,DiscoveryDatabaseState> allCoreStatesForDatabase( NamedDatabaseId namedDatabaseId );

    Map<ServerId,DiscoveryDatabaseState> allReadReplicaStatesForDatabase( NamedDatabaseId namedDatabaseId );

    boolean isHealthy();

    RaftMemberId resolveRaftMemberForServer( NamedDatabaseId namedDatabaseId, ServerId serverId );

    ServerId resolveServerForRaftMember( RaftMemberId raftMemberId );
}
