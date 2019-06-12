/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;

import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Provides a read-only service for the eventually consistent topology information.
 */
public interface TopologyService extends Lifecycle
{
    void onDatabaseStart( DatabaseId databaseId );

    void onDatabaseStop( DatabaseId databaseId );

    Map<MemberId,CoreServerInfo> allCoreServers();

    DatabaseCoreTopology coreTopologyForDatabase( DatabaseId databaseId );

    Map<MemberId,ReadReplicaInfo> allReadReplicas();

    DatabaseReadReplicaTopology readReplicaTopologyForDatabase( DatabaseId databaseId );

    AdvertisedSocketAddress findCatchupAddress( MemberId upstream ) throws CatchupAddressResolutionException;

    RoleInfo coreRole( DatabaseId databaseId, MemberId memberId );

    MemberId memberId();
}
