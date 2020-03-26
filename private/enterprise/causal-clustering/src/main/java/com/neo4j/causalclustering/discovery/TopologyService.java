/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.core.consensus.LeaderInfo;
import com.neo4j.causalclustering.discovery.akka.database.state.DiscoveryDatabaseState;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.dbms.DatabaseStateChangedListener;

import java.util.Map;

import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Provides a read-only service for the eventually consistent topology information.
 */
public interface TopologyService extends Lifecycle, DatabaseStateChangedListener
{
    void onDatabaseStart( NamedDatabaseId namedDatabaseId );

    void onDatabaseStop( NamedDatabaseId namedDatabaseId );

    MemberId memberId();

    Map<MemberId,CoreServerInfo> allCoreServers();

    Map<MemberId,ReadReplicaInfo> allReadReplicas();

    DatabaseCoreTopology coreTopologyForDatabase( NamedDatabaseId namedDatabaseId );

    DatabaseReadReplicaTopology readReplicaTopologyForDatabase( NamedDatabaseId namedDatabaseId );

    SocketAddress lookupCatchupAddress( MemberId upstream ) throws CatchupAddressResolutionException;

    LeaderInfo getLeader( NamedDatabaseId namedDatabaseId );

    RoleInfo lookupRole( NamedDatabaseId namedDatabaseId, MemberId memberId );

    DiscoveryDatabaseState lookupDatabaseState( NamedDatabaseId namedDatabaseId, MemberId memberId );

    Map<MemberId,DiscoveryDatabaseState> allCoreStatesForDatabase( NamedDatabaseId namedDatabaseId );

    Map<MemberId,DiscoveryDatabaseState> allReadReplicaStatesForDatabase( NamedDatabaseId namedDatabaseId );
}
