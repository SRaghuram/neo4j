/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.core.consensus.leader_transfer.LeaderTransferService;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.identity.ClusteringIdentityModule;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Outbound;
import com.neo4j.configuration.ServerGroupsSupplier;

import java.util.function.Function;

import org.neo4j.collection.Dependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;

public class RaftGroupFactory
{
    private final ClusteringIdentityModule identityModule;
    private final GlobalModule globalModule;
    private final ClusterStateLayout clusterState;
    private final CoreTopologyService topologyService;
    private final ClusterStateStorageFactory storageFactory;
    private final LeaderTransferService leaderTransferService;
    private final Function<NamedDatabaseId,LeaderListener> listenerFactory;
    private final MemoryTracker memoryTracker;
    private final ServerGroupsSupplier serverGroupsSupplier;

    public RaftGroupFactory( ClusteringIdentityModule identityModule, GlobalModule globalModule, ClusterStateLayout clusterState,
            CoreTopologyService topologyService, ClusterStateStorageFactory storageFactory, LeaderTransferService leaderTransferService,
            Function<NamedDatabaseId,LeaderListener> listenerFactory, MemoryTracker memoryTracker,
            ServerGroupsSupplier serverGroupsSupplier )
    {
        this.identityModule = identityModule;
        this.globalModule = globalModule;
        this.clusterState = clusterState;
        this.topologyService = topologyService;
        this.storageFactory = storageFactory;
        this.leaderTransferService = leaderTransferService;
        this.listenerFactory = listenerFactory;
        this.memoryTracker = memoryTracker;
        this.serverGroupsSupplier = serverGroupsSupplier;
    }

    public RaftGroup create( NamedDatabaseId namedDatabaseId, Outbound<MemberId,RaftMessages.RaftMessage> outbound, LifeSupport life, Monitors monitors,
            Dependencies dependencies, DatabaseLogService logService )
    {
        // TODO: Consider if additional services are per raft group, e.g. config, log-service.
        return new RaftGroup( globalModule.getGlobalConfig(), logService, globalModule.getFileSystem(), globalModule.getJobScheduler(),
                globalModule.getGlobalClock(), identityModule.memberId( namedDatabaseId ), life, monitors, dependencies, outbound, clusterState,
                topologyService, storageFactory, namedDatabaseId, leaderTransferService, listenerFactory.apply( namedDatabaseId ), memoryTracker,
                serverGroupsSupplier );
    }
}
