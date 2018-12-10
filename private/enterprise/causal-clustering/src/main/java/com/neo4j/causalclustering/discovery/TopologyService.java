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
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Provides a read-only service for the eventually consistent topology information.
 */
public interface TopologyService extends Lifecycle
{
    String localDBName();

    // It is perhaps confusing (Or even error inducing) that this core Topology will always contain the cluster id
    // for the database local to the host upon which this method is called.
    // TODO: evaluate returning clusterId = null for global Topologies returned by allCoreServers()
    CoreTopology allCoreServers();

    CoreTopology localCoreServers();

    ReadReplicaTopology allReadReplicas();

    ReadReplicaTopology localReadReplicas();

    AdvertisedSocketAddress findCatchupAddress( MemberId upstream ) throws CatchupAddressResolutionException;

    Map<MemberId,RoleInfo> allCoreRoles();

    MemberId myself();
}
