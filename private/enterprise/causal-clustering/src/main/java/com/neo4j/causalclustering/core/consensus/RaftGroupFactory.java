/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.common.state.ClusterStateStorageFactory;
import com.neo4j.causalclustering.core.consensus.leader_transfer.LeaderTransferService;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.causalclustering.identity.RaftMemberId;
import com.neo4j.causalclustering.messaging.Outbound;
import com.neo4j.configuration.ServerGroupsSupplier;
import com.neo4j.dbms.database.DbmsLogEntryWriterProvider;

import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.helpers.ReadOnlyDatabaseChecker;
import org.neo4j.function.Suppliers.Lazy;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.DatabaseLogService;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.Monitors;

public class RaftGroupFactory
{
    private final GlobalModule globalModule;
    private final ClusterStateLayout clusterState;
    private final ClusterStateStorageFactory storageFactory;
    private final LeaderTransferService leaderTransferService;
    private final Function<NamedDatabaseId,LeaderListener> listenerFactory;
    private final MemoryTracker memoryTracker;
    private final ServerGroupsSupplier serverGroupsSupplier;
    private final DbmsLogEntryWriterProvider dbmsLogEntryWriterProvider;

    public RaftGroupFactory( GlobalModule globalModule, ClusterStateLayout clusterState, ClusterStateStorageFactory storageFactory,
            LeaderTransferService leaderTransferService, Function<NamedDatabaseId,LeaderListener> listenerFactory, MemoryTracker memoryTracker,
            ServerGroupsSupplier serverGroupsSupplier, DbmsLogEntryWriterProvider dbmsLogEntryWriterProvider )
    {
        this.globalModule = globalModule;
        this.clusterState = clusterState;
        this.storageFactory = storageFactory;
        this.leaderTransferService = leaderTransferService;
        this.listenerFactory = listenerFactory;
        this.memoryTracker = memoryTracker;
        this.serverGroupsSupplier = serverGroupsSupplier;
        this.dbmsLogEntryWriterProvider = dbmsLogEntryWriterProvider;
    }

    public RaftGroup create( NamedDatabaseId namedDatabaseId, Lazy<RaftMemberId> raftMemberId,
            Outbound<RaftMemberId,RaftMessages.RaftMessage> outbound, LifeSupport life, Monitors monitors, Dependencies dependencies,
            DatabaseLogService logService, Consumer<RaftMessages.StatusResponse> statusResponseConsumer,
            ReadOnlyDatabaseChecker readOnlyDatabaseChecker )
    {
        // TODO: Consider if additional services are per raft group, e.g. config, log-service.
        return new RaftGroup( globalModule.getGlobalConfig(), logService, globalModule.getFileSystem(), globalModule.getJobScheduler(),
                              globalModule.getGlobalClock(), raftMemberId, life, monitors, dependencies, outbound, clusterState,
                              storageFactory, namedDatabaseId, leaderTransferService, listenerFactory.apply( namedDatabaseId ), memoryTracker,
                              serverGroupsSupplier, globalModule.getGlobalAvailabilityGuard(), statusResponseConsumer,
                              dbmsLogEntryWriterProvider.getEntryWriterFactory( namedDatabaseId ), readOnlyDatabaseChecker );
    }
}
