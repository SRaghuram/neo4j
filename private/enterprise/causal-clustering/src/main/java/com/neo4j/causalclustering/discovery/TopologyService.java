/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.catchup.CatchupAddressResolutionException;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Map;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.Lifecycle;

// todo: use DatabaseId instead of String

/**
 * Provides a read-only service for the eventually consistent topology information.
 */
public interface TopologyService extends Lifecycle
{
    DatabaseId localDatabaseId();

    Map<MemberId,CoreServerInfo> allCoreServers();

    CoreTopology coreServersForDatabase( DatabaseId databaseId );

    Map<MemberId,ReadReplicaInfo> allReadReplicas();

    ReadReplicaTopology readReplicasForDatabase( DatabaseId databaseId );

    AdvertisedSocketAddress findCatchupAddress( MemberId upstream ) throws CatchupAddressResolutionException;

    Map<MemberId,RoleInfo> allCoreRoles();

    MemberId myself();
}
