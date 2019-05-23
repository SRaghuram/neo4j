/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.consensus.membership.RaftMembershipState;
import com.neo4j.causalclustering.core.consensus.term.TermState;
import com.neo4j.causalclustering.core.consensus.vote.VoteState;
import com.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import com.neo4j.causalclustering.core.state.machines.id.IdAllocationState;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import com.neo4j.causalclustering.core.state.storage.DurableStateStorage;
import com.neo4j.causalclustering.core.state.storage.SimpleFileStorage;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.core.state.storage.StateStorage;
import com.neo4j.causalclustering.core.state.version.ClusterStateVersion;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;

import java.io.File;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.DatabaseLogProvider;

public class CoreStateStorageFactory
{
    private final FileSystemAbstraction fs;
    private final LogProvider globalLogProvider;
    private final ClusterStateLayout layout;
    private final Config config;

    public CoreStateStorageFactory( FileSystemAbstraction fs, ClusterStateLayout layout, LogProvider globalLogProvider, Config config )
    {
        this.fs = fs;
        this.globalLogProvider = globalLogProvider;
        this.layout = layout;
        this.config = config;
    }

    public SimpleStorage<ClusterStateVersion> createClusterStateVersionStorage()
    {
        return createSimpleStorage( layout.clusterStateVersionFile(), CoreStateFiles.VERSION, globalLogProvider );
    }

    public SimpleStorage<MemberId> createMemberIdStorage()
    {
        return createSimpleStorage( layout.memberIdStateFile(), CoreStateFiles.CORE_MEMBER_ID, globalLogProvider );
    }

    public SimpleStorage<RaftId> createRaftIdStorage( DatabaseId databaseId, DatabaseLogProvider logProvider )
    {
        return createSimpleStorage( layout.raftIdStateFile( databaseId ), CoreStateFiles.RAFT_ID, logProvider );
    }

    public StateStorage<IdAllocationState> createIdAllocationStorage( DatabaseId databaseId, LifeSupport life, DatabaseLogProvider logProvider )
    {
        return createDurableStorage( layout.idAllocationStateDirectory( databaseId ), CoreStateFiles.ID_ALLOCATION, life, logProvider );
    }

    public StateStorage<ReplicatedLockTokenState> createLockTokenStorage( DatabaseId databaseId, LifeSupport life, DatabaseLogProvider logProvider )
    {
        return createDurableStorage( layout.lockTokenStateDirectory( databaseId ), CoreStateFiles.LOCK_TOKEN, life, logProvider );
    }

    public StateStorage<Long> createLastFlushedStorage( DatabaseId databaseId, LifeSupport life, DatabaseLogProvider logProvider )
    {
        return createDurableStorage( layout.lastFlushedStateDirectory( databaseId ), CoreStateFiles.LAST_FLUSHED, life, logProvider );
    }

    public StateStorage<RaftMembershipState> createRaftMembershipStorage( DatabaseId databaseId, LifeSupport life, DatabaseLogProvider logProvider )
    {
        return createDurableStorage( layout.raftMembershipStateDirectory( databaseId ), CoreStateFiles.RAFT_MEMBERSHIP, life, logProvider );
    }

    public StateStorage<GlobalSessionTrackerState> createSessionTrackerStorage( DatabaseId databaseId, LifeSupport life, DatabaseLogProvider logProvider )
    {
        return createDurableStorage( layout.sessionTrackerDirectory( databaseId ), CoreStateFiles.SESSION_TRACKER, life, logProvider );
    }

    public StateStorage<TermState> createRaftTermStorage( DatabaseId databaseId, LifeSupport life, DatabaseLogProvider logProvider )
    {
        return createDurableStorage( layout.raftTermStateDirectory( databaseId ), CoreStateFiles.RAFT_TERM, life, logProvider );
    }

    public StateStorage<VoteState> createRaftVoteStorage( DatabaseId databaseId, LifeSupport life, DatabaseLogProvider logProvider )
    {
        return createDurableStorage( layout.raftVoteStateDirectory( databaseId ), CoreStateFiles.RAFT_VOTE, life, logProvider );
    }

    private <T> SimpleStorage<T> createSimpleStorage( File file, CoreStateFiles<T> type, LogProvider logProvider )
    {
        return new SimpleFileStorage<>( fs, file, type.marshal(), logProvider );
    }

    private <T> StateStorage<T> createDurableStorage( File directory, CoreStateFiles<T> type, LifeSupport life, LogProvider logProvider )
    {
        DurableStateStorage<T> storage = new DurableStateStorage<>( fs, directory, type, type.rotationSize( config ), logProvider );
        life.add( storage );
        return storage;
    }

    public ClusterStateLayout layout()
    {
        return layout;
    }
}
