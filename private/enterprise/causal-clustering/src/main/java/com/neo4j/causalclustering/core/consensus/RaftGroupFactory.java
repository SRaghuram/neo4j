/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.causalclustering.core.state.CoreStateStorageFactory;
import com.neo4j.causalclustering.discovery.CoreTopologyService;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Outbound;

import org.neo4j.collection.Dependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.monitoring.Monitors;

public class RaftGroupFactory
{
    private final MemberId myself;
    private final GlobalModule globalModule;
    private final Outbound<MemberId,RaftMessages.RaftMessage> outbound;
    private final ClusterStateLayout clusterState;
    private final CoreTopologyService topologyService;
    private final CoreStateStorageFactory storageFactory;

    public RaftGroupFactory( MemberId myself, final GlobalModule globalModule, Outbound<MemberId,RaftMessages.RaftMessage> outbound,
            ClusterStateLayout clusterState, CoreTopologyService topologyService, CoreStateStorageFactory storageFactory )
    {
        this.myself = myself;
        this.globalModule = globalModule;
        this.outbound = outbound;
        this.clusterState = clusterState;
        this.topologyService = topologyService;
        this.storageFactory = storageFactory;
    }

    public RaftGroup create( DatabaseId databaseId, LifeSupport life, Monitors monitors, Dependencies dependencies )
    {
        // TODO: Consider if additional services are per raft group, e.g. config, log-service.
        return new RaftGroup( globalModule.getGlobalConfig(), globalModule.getLogService(), globalModule.getFileSystem(), globalModule.getJobScheduler(),
                globalModule.getGlobalClock(), myself, life, monitors, dependencies, outbound, clusterState, topologyService, storageFactory, databaseId );
    }
}
