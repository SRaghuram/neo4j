/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common.state;

import com.neo4j.causalclustering.core.consensus.membership.RaftMembershipState;
import com.neo4j.causalclustering.core.consensus.term.TermState;
import com.neo4j.causalclustering.core.consensus.vote.VoteState;
import com.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import com.neo4j.causalclustering.core.state.ClusterStateLayout;
import com.neo4j.causalclustering.core.state.CoreStateFiles;
import com.neo4j.causalclustering.core.state.machines.lease.ReplicatedLeaseState;
import com.neo4j.causalclustering.core.state.storage.DurableStateStorage;
import com.neo4j.causalclustering.core.state.version.ClusterStateVersion;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;

import java.io.File;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.state.SimpleFileStorage;
import org.neo4j.io.state.SimpleStorage;
import org.neo4j.io.state.StateStorage;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.DatabaseLogProvider;
import org.neo4j.memory.MemoryTracker;

public class ClusterStateStorageFactory
{
    private final FileSystemAbstraction fs;
    private final LogProvider globalLogProvider;
    private final ClusterStateLayout layout;
    private final Config config;
    private final MemoryTracker memoryTracker;

    public ClusterStateStorageFactory( FileSystemAbstraction fs, ClusterStateLayout layout, LogProvider globalLogProvider, Config config,
            MemoryTracker memoryTracker )
    {
        this.fs = fs;
        this.globalLogProvider = globalLogProvider;
        this.layout = layout;
        this.config = config;
        this.memoryTracker = memoryTracker;
    }

    public SimpleStorage<ClusterStateVersion> createClusterStateVersionStorage()
    {
        return createSimpleStorage( layout.clusterStateVersionFile(), CoreStateFiles.VERSION, globalLogProvider );
    }

    public SimpleStorage<MemberId> createMemberIdStorage()
    {
        return createSimpleStorage( layout.memberIdStateFile(), CoreStateFiles.CORE_MEMBER_ID, globalLogProvider );
    }

    public SimpleStorage<RaftId> createRaftIdStorage( String databaseName, DatabaseLogProvider logProvider )
    {
        return createSimpleStorage( layout.raftIdStateFile( databaseName ), CoreStateFiles.RAFT_ID, logProvider );
    }

    public StateStorage<ReplicatedLeaseState> createLeaseStorage( String databaseName, LifeSupport life, DatabaseLogProvider logProvider )
    {
        return createDurableStorage( layout.leaseStateDirectory( databaseName ), CoreStateFiles.LEASE, life, logProvider );
    }

    public StateStorage<Long> createLastFlushedStorage( String databaseName, LifeSupport life, DatabaseLogProvider logProvider )
    {
        return createDurableStorage( layout.lastFlushedStateDirectory( databaseName ), CoreStateFiles.LAST_FLUSHED, life, logProvider );
    }

    public StateStorage<RaftMembershipState> createRaftMembershipStorage( String databaseName, LifeSupport life, DatabaseLogProvider logProvider )
    {
        return createDurableStorage( layout.raftMembershipStateDirectory( databaseName ), CoreStateFiles.RAFT_MEMBERSHIP, life, logProvider );
    }

    public StateStorage<GlobalSessionTrackerState> createSessionTrackerStorage( String databaseName, LifeSupport life, DatabaseLogProvider logProvider )
    {
        return createDurableStorage( layout.sessionTrackerDirectory( databaseName ), CoreStateFiles.SESSION_TRACKER, life, logProvider );
    }

    public StateStorage<TermState> createRaftTermStorage( String databaseName, LifeSupport life, DatabaseLogProvider logProvider )
    {
        return createDurableStorage( layout.raftTermStateDirectory( databaseName ), CoreStateFiles.RAFT_TERM, life, logProvider );
    }

    public StateStorage<VoteState> createRaftVoteStorage( String databaseName, LifeSupport life, DatabaseLogProvider logProvider )
    {
        return createDurableStorage( layout.raftVoteStateDirectory( databaseName ), CoreStateFiles.RAFT_VOTE, life, logProvider );
    }

    private <T> SimpleStorage<T> createSimpleStorage( File file, CoreStateFiles<T> type, LogProvider logProvider )
    {
        return new SimpleFileStorage<>( fs, file, type.marshal(), memoryTracker );
    }

    private <T> StateStorage<T> createDurableStorage( File directory, CoreStateFiles<T> type, LifeSupport life, LogProvider logProvider )
    {
        DurableStateStorage<T> storage = new DurableStateStorage<>( fs, directory, type, type.rotationSize( config ), logProvider, memoryTracker );
        life.add( storage );
        return storage;
    }

    public ClusterStateLayout layout()
    {
        return layout;
    }
}
