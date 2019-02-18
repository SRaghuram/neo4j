/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering;

import com.neo4j.causalclustering.common.DatabaseService;
import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.replication.ProgressTrackerImpl;
import com.neo4j.causalclustering.core.replication.RaftReplicator;
import com.neo4j.causalclustering.core.replication.session.GlobalSession;
import com.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import com.neo4j.causalclustering.core.replication.session.LocalSessionPool;
import com.neo4j.causalclustering.core.state.CoreStateFiles;
import com.neo4j.causalclustering.core.state.CoreStateStorageService;
import com.neo4j.causalclustering.core.state.storage.StateStorage;
import com.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import com.neo4j.causalclustering.helper.TimeoutStrategy;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.messaging.Outbound;

import java.time.Duration;
import java.util.UUID;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.logging.LogProvider;

public class ReplicationModule
{
    private final RaftReplicator replicator;
    private final ProgressTrackerImpl progressTracker;
    private final SessionTracker sessionTracker;

    public ReplicationModule( RaftMachine raftMachine, MemberId myself, GlobalModule globalModule, Config config,
            Outbound<MemberId,RaftMessages.RaftMessage> outbound, CoreStateStorageService stateStorageService, LogProvider logProvider,
            AvailabilityGuard globalAvailabilityGuard, DatabaseService localDatabases )
    {
        StateStorage<GlobalSessionTrackerState> sessionTrackerStorage = stateStorageService.stateStorage( CoreStateFiles.SESSION_TRACKER );

        sessionTracker = new SessionTracker( sessionTrackerStorage );

        GlobalSession myGlobalSession = new GlobalSession( UUID.randomUUID(), myself );
        LocalSessionPool sessionPool = new LocalSessionPool( myGlobalSession );
        progressTracker = new ProgressTrackerImpl( myGlobalSession );

        Duration initialBackoff = config.get( CausalClusteringSettings.replication_retry_timeout_base );
        Duration upperBoundBackoff = config.get( CausalClusteringSettings.replication_retry_timeout_limit );

        TimeoutStrategy progressRetryStrategy = new ExponentialBackoffStrategy( initialBackoff, upperBoundBackoff );
        long availabilityTimeoutMillis = config.get( CausalClusteringSettings.replication_retry_timeout_base ).toMillis();
        replicator = new RaftReplicator(
                raftMachine,
                myself,
                outbound,
                sessionPool,
                progressTracker, progressRetryStrategy, availabilityTimeoutMillis,
                globalAvailabilityGuard, logProvider, localDatabases,
                globalModule.getGlobalMonitors() );
    }

    public RaftReplicator getReplicator()
    {
        return replicator;
    }

    public ProgressTrackerImpl getProgressTracker()
    {
        return progressTracker;
    }

    public SessionTracker getSessionTracker()
    {
        return sessionTracker;
    }
}
