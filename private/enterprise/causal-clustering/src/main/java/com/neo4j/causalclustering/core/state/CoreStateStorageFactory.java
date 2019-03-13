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
import com.neo4j.causalclustering.core.state.storage.RotatingStorage;
import com.neo4j.causalclustering.core.state.storage.SimpleFileStorage;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.DatabaseName;
import com.neo4j.causalclustering.identity.MemberId;

import java.io.File;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
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

    public RotatingStorage<IdAllocationState> createIdAllocationStorage( String databaseName )
    {
        return createRotatingStorage( layout.idAllocationStateDirectory( databaseName ), CoreStateFiles.ID_ALLOCATION );
    }

    public RotatingStorage<ReplicatedLockTokenState> createLockTokenStorage( String databaseName )
    {
        return createRotatingStorage( layout.lockTokenStateDirectory( databaseName ), CoreStateFiles.LOCK_TOKEN );
    }

    public RotatingStorage<Long> createLastFlushedStorage( String databaseName )
    {
        return createRotatingStorage( layout.lastFlushedStateDirectory( databaseName ), CoreStateFiles.LAST_FLUSHED );
    }

    public RotatingStorage<RaftMembershipState> createRaftMembershipStorage( String databaseName )
    {
        return createRotatingStorage( layout.raftMembershipStateDirectory( databaseName ), CoreStateFiles.RAFT_MEMBERSHIP );
    }

    public RotatingStorage<GlobalSessionTrackerState> createSessionTrackerStorage( String databaseName )
    {
        return createRotatingStorage( layout.sessionTrackerDirectory( databaseName ), CoreStateFiles.SESSION_TRACKER );
    }

    public RotatingStorage<TermState> createRaftTermStorage( String databaseName )
    {
        return createRotatingStorage( layout.raftTermStateDirectory( databaseName ), CoreStateFiles.RAFT_TERM );
    }

    public RotatingStorage<VoteState> createRaftVoteStorage( String databaseName )
    {
        return createRotatingStorage( layout.raftVoteStateDirectory( databaseName ), CoreStateFiles.RAFT_VOTE );
    }

    private <T> SimpleStorage<T> createSimpleStorage( File file, CoreStateFiles<T> type )
    {
        return new SimpleFileStorage<>( fs, file, type.marshal(), logProvider );
    }

    private <T> RotatingStorage<T> createRotatingStorage( File directory, CoreStateFiles<T> type )
    {
        return new DurableStateStorage<>( fs, directory, type, type.rotationSize( config ), logProvider );
    }
}
