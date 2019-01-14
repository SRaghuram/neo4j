/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import java.io.File;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import org.neo4j.causalclustering.core.consensus.log.RaftLog;
import org.neo4j.causalclustering.core.consensus.log.segmented.SegmentedRaftLog;
import org.neo4j.causalclustering.core.consensus.membership.RaftMembershipState;
import org.neo4j.causalclustering.core.consensus.term.TermState;
import org.neo4j.causalclustering.core.consensus.vote.VoteState;
import org.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import org.neo4j.causalclustering.core.state.machines.id.IdAllocationState;
import org.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import org.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import org.neo4j.causalclustering.core.state.storage.DurableStateStorage;
import org.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import org.neo4j.causalclustering.core.state.storage.SimpleFileStorage;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.DatabaseName;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Config;

import static java.util.Arrays.asList;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.global_session_tracker_state_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.id_alloc_state_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.last_flushed_state_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_membership_state_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.replicated_lock_token_state_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.term_state_size;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.vote_state_size;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.FileType.DUAL;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.FileType.NONE;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.FileType.SEGMENTED;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.FileType.SIMPLE;

/**
 * Enumerates and categorises core cluster state files.
 *
 * @param <STATE> The state type.
 */
@SuppressWarnings( {"WeakerAccess", "deprecation"} )
public class CoreStateFiles<STATE>
{
    enum FileType
    {
        /**
         * @see SimpleFileStorage
         */
        SIMPLE( baseName -> baseName + "-state" ),
        /**
         * @see DurableStateStorage
         */
        DUAL( baseName -> baseName + "-state" ),
        /**
         * @see SegmentedRaftLog
         */
        SEGMENTED( Function.identity() ),
        /**
         * Some core state is not persisted to its own file in cluster-state. Primarily {{@link RaftCoreState}}
         */
        NONE( ignored -> null );

        private final Function<String,String> directoryFunction;

        FileType( Function<String,String> directoryFunction )
        {
            this.directoryFunction = directoryFunction;
        }
    }

    public static final CoreStateFiles<ReplicatedLockTokenState> LOCK_TOKEN =
            new CoreStateFiles<>( "lock-token", DUAL, new ReplicatedLockTokenState.Marshal(), replicated_lock_token_state_size, CoreStateType.LOCK_TOKEN );
    public static final CoreStateFiles<GlobalSessionTrackerState> SESSION_TRACKER =
            new CoreStateFiles<>( "session-tracker", DUAL, new GlobalSessionTrackerState.Marshal(), global_session_tracker_state_size,
                    CoreStateType.SESSION_TRACKER );
    public static final CoreStateFiles<IdAllocationState> ID_ALLOCATION =
            new CoreStateFiles<>( "id-allocation", DUAL, new IdAllocationState.Marshal(), id_alloc_state_size, CoreStateType.ID_ALLOCATION );
    public static final CoreStateFiles<RaftCoreState> RAFT_CORE_STATE =
            new CoreStateFiles<>( "core", NONE, new RaftCoreState.Marshal(), CoreStateType.RAFT_CORE_STATE );
    public static final CoreStateFiles<DatabaseName> DB_NAME = new CoreStateFiles<>( "db-name", SIMPLE, new DatabaseName.Marshal(), CoreStateType.DB_NAME );
    public static final CoreStateFiles<ClusterId> CLUSTER_ID = new CoreStateFiles<>( "cluster-id", SIMPLE, new ClusterId.Marshal(), CoreStateType.CLUSTER_ID );
    public static final CoreStateFiles<MemberId> CORE_MEMBER_ID =
            new CoreStateFiles<>( "core-member-id", SIMPLE, new MemberId.Marshal(), CoreStateType.CORE_MEMBER_ID );
    public static final CoreStateFiles<RaftLog> RAFT_LOG = new CoreStateFiles<>( "raft-log", SEGMENTED, null, CoreStateType.RAFT_LOG );
    public static final CoreStateFiles<TermState> RAFT_TERM =
            new CoreStateFiles<>( "term", DUAL, new TermState.Marshal(), term_state_size, CoreStateType.RAFT_TERM );
    public static final CoreStateFiles<VoteState> RAFT_VOTE =
            new CoreStateFiles<>( "vote", DUAL, new VoteState.Marshal(), vote_state_size, CoreStateType.RAFT_VOTE );
    public static final CoreStateFiles<RaftMembershipState> RAFT_MEMBERSHIP =
            new CoreStateFiles<>( "membership", DUAL, new RaftMembershipState.Marshal(), raft_membership_state_size, CoreStateType.RAFT_MEMBERSHIP );
    public static final CoreStateFiles<Long> LAST_FLUSHED =
            new CoreStateFiles<>( "last-flushed", DUAL, new LongIndexMarshal(), last_flushed_state_size, CoreStateType.LAST_FLUSHED );

    // for testing purposes
    public static <S> CoreStateFiles<S> DUMMY( SafeStateMarshal<S> marshal )
    {
        return new CoreStateFiles<>( "dummy", SIMPLE, marshal, CoreStateType.DUMMY );
    }

    private static List<CoreStateFiles<?>> values;

    static
    {
        values = asList( ID_ALLOCATION, LOCK_TOKEN, DB_NAME, CLUSTER_ID, CORE_MEMBER_ID, RAFT_LOG, RAFT_TERM, RAFT_VOTE, RAFT_MEMBERSHIP, RAFT_CORE_STATE,
                LAST_FLUSHED, SESSION_TRACKER );
        values.sort( Comparator.comparingInt( CoreStateFiles::typeId ) );
        values = Collections.unmodifiableList( values );
    }

    public static List<CoreStateFiles<?>> values()
    {
        return values;
    }

    private final String baseName;
    private final FileType fileType;
    private final SafeStateMarshal<STATE> marshal;
    private final Setting<Integer> rotationSizeSetting;
    private final CoreStateType typeId;

    private CoreStateFiles( String baseName, FileType fileType, SafeStateMarshal<STATE> marshal, CoreStateType typeId )
    {
        this( baseName, fileType, marshal, null, typeId );
    }

    private CoreStateFiles( String baseName, FileType fileType, SafeStateMarshal<STATE> marshal, Setting<Integer> rotationSizeSetting, CoreStateType typeId )
    {
        this.baseName = baseName;
        this.fileType = fileType;
        this.marshal = marshal;
        this.typeId = typeId;
        this.rotationSizeSetting = rotationSizeSetting;
    }

    public File at( File root )
    {
        return new File( root, directoryName() );
    }

    public String baseName()
    {
        return baseName;
    }

    public String directoryName()
    {
        return fileType.directoryFunction.apply( baseName );
    }

    public FileType fileType()
    {
        return fileType;
    }

    public int rotationSize( Config config )
    {
        if ( rotationSizeSetting == null )
        {
            throw new UnsupportedOperationException( "This type does not rotate and thus has no rotation size setting: " + this );
        }
        return config.get( rotationSizeSetting );
    }

    public SafeStateMarshal<STATE> marshal()
    {
        if ( marshal == null )
        {
            throw new UnsupportedOperationException( "This type does not have a marshal registered." + this );
        }
        return marshal;
    }

    public int typeId()
    {
        return typeId.typeId();
    }

    @Override
    public String toString()
    {
        return directoryName();
    }
}
