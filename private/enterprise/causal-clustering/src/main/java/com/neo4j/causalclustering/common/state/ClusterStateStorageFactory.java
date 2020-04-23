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
import com.neo4j.causalclustering.core.state.storage.SimpleFileStorage;
import com.neo4j.causalclustering.core.state.storage.SimpleStorage;
import com.neo4j.causalclustering.core.state.storage.StateStorage;
import com.neo4j.causalclustering.core.state.version.ClusterStateVersion;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.identity.RaftId;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.DatabaseLog;
import org.neo4j.logging.internal.DatabaseLogProvider;
import org.neo4j.memory.MemoryTracker;

import static java.lang.String.format;

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
        return new SimpleFileStorage<>( fs, file, type.marshal(), logProvider, memoryTracker );
    }

    private <T> StateStorage<T> createDurableStorage( File directory, CoreStateFiles<T> type, LifeSupport life, LogProvider logProvider )
    {
        DurableStateStorage<T> storage = new DurableStateStorage<>( fs, directory, type, type.rotationSize( config ), logProvider, memoryTracker );
        life.add( storage );
        return storage;
    }

    /**
     * Check that the cluster state directory does not contain cluster state for a previous database of the same name.
     * If it does, then do our best to delete all said cluster state.
     * @param id the id of the new database to be created
     * @param logProvider the logger for this new database
     */
    public void clearFor( NamedDatabaseId id, DatabaseLogProvider logProvider ) throws IOException
    {
        File clusterStateForDb = layout.raftGroupDir( id.name() );
        if ( !fs.fileExists( clusterStateForDb ) )
        {
            return;
        }

        var idStateFile = layout.raftIdStateFile( id.name() );
        if ( idStateFile.exists() )
        {
            var raftIdSimpleStorage = createSimpleStorage( layout.raftIdStateFile( id.name() ), CoreStateFiles.RAFT_ID, logProvider );
            RaftId raftId = raftIdSimpleStorage.readState();

            if ( !Objects.equals( id.databaseId().uuid(), raftId.uuid() ) )
            {
                DatabaseLog log = logProvider.getLog( getClass() );
                log.warn( format( "There was orphan cluster state belonging to a previous database %s with a different id {Old:%s New:%s} " +
                        "This likely means a previous DROP did not complete successfully.",
                        id.name(), raftId.uuid(), id.databaseId().uuid() ) );
                FileSystemUtils.deleteFile( fs, clusterStateForDb );
            }
        }
    }

    public ClusterStateLayout layout()
    {
        return layout;
    }
}
