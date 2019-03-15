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
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.DatabaseName;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.File;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;

public class CoreStateStorageFactory
{
    private final FileSystemAbstraction fs;
    private final LogProvider logProvider;
    private final ClusterStateLayout layout;
    private final Config config;

    public CoreStateStorageFactory( FileSystemAbstraction fs, ClusterStateLayout layout, LogProvider logProvider, Config config )
    {
        this.fs = fs;
        this.logProvider = logProvider;
        this.layout = layout;
        this.config = config;
    }

    public SimpleStorage<ClusterId> createClusterIdStorage()
    {
        return createSimpleStorage( layout.clusterIdStateFile(), CoreStateFiles.CLUSTER_ID );
    }

    public SimpleStorage<MemberId> createMemberIdStorage()
    {
        return createSimpleStorage( layout.memberIdStateFile(), CoreStateFiles.CORE_MEMBER_ID );
    }

    public SimpleStorage<DatabaseName> createMultiClusteringDbNameStorage()
    {
        return createSimpleStorage( layout.multiClusteringDbNameStateFile(), CoreStateFiles.DB_NAME );
    }

    public StateStorage<IdAllocationState> createIdAllocationStorage( String databaseName, LifeSupport life )
    {
        return createDurableStorage( layout.idAllocationStateDirectory( databaseName ), CoreStateFiles.ID_ALLOCATION, life );
    }

    public StateStorage<ReplicatedLockTokenState> createLockTokenStorage( String databaseName, LifeSupport life )
    {
        return createDurableStorage( layout.lockTokenStateDirectory( databaseName ), CoreStateFiles.LOCK_TOKEN, life );
    }

    public StateStorage<Long> createLastFlushedStorage( String databaseName, LifeSupport life )
    {
        return createDurableStorage( layout.lastFlushedStateDirectory( databaseName ), CoreStateFiles.LAST_FLUSHED, life );
    }

    public StateStorage<RaftMembershipState> createRaftMembershipStorage( String databaseName, LifeSupport life )
    {
        return createDurableStorage( layout.raftMembershipStateDirectory( databaseName ), CoreStateFiles.RAFT_MEMBERSHIP, life );
    }

    public StateStorage<GlobalSessionTrackerState> createSessionTrackerStorage( String databaseName, LifeSupport life )
    {
        return createDurableStorage( layout.sessionTrackerDirectory( databaseName ), CoreStateFiles.SESSION_TRACKER, life );
    }

    public StateStorage<TermState> createRaftTermStorage( String databaseName, LifeSupport life )
    {
        return createDurableStorage( layout.raftTermStateDirectory( databaseName ), CoreStateFiles.RAFT_TERM, life );
    }

    public StateStorage<VoteState> createRaftVoteStorage( String databaseName, LifeSupport life )
    {
        return createDurableStorage( layout.raftVoteStateDirectory( databaseName ), CoreStateFiles.RAFT_VOTE, life );
    }

    private <T> SimpleStorage<T> createSimpleStorage( File file, CoreStateFiles<T> type )
    {
        return new SimpleFileStorage<>( fs, file, type.marshal(), logProvider );
    }

    private <T> StateStorage<T> createDurableStorage( File directory, CoreStateFiles<T> type, LifeSupport life )
    {
        DurableStateStorage<T> storage = new DurableStateStorage<>( fs, directory, type, type.rotationSize( config ), logProvider );
        life.add( storage );
        return storage;
    }
}
