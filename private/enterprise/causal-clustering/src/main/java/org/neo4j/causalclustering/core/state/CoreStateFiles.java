/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
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
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

import static org.neo4j.causalclustering.core.state.CoreStateFiles.FileType.DUAL;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.FileType.NONE;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.FileType.SEGMENTED;
import static org.neo4j.causalclustering.core.state.CoreStateFiles.FileType.SIMPLE;

//TODO: This class is useful, but needs heavy doc block
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

        private final Function<String,String> dir;

        FileType( Function<String,String> dir )
        {
            this.dir = dir;
        }
    }

    private static SafeStateMarshal<RaftLog> unsupportedRaftLogMarshal = new SafeStateMarshal<RaftLog>()
    {
        @Override
        protected RaftLog unmarshal0( ReadableChannel channel )
        {
            throw new UnsupportedOperationException( "You cannot directly unmarshal a RaftLog" );
        }

        @Override
        public RaftLog startState()
        {
            return null;
        }

        @Override
        public long ordinal( RaftLog log )
        {
            return 0;
        }

        @Override
        public void marshal( RaftLog log, WritableChannel channel ) throws IOException
        {
            throw new IOException( "You cannot directly marshal a RaftLog!" );
        }
    };

    //Ordinals for existing CoreState types must be in this order for backwards compatibility with 3.4
    public static final CoreStateFiles<ReplicatedLockTokenState> LOCK_TOKEN =
            new CoreStateFiles<>( "lock-token", DUAL, new ReplicatedLockTokenState.Marshal(), CausalClusteringSettings.replicated_lock_token_state_size, 0 );
    public static final CoreStateFiles<GlobalSessionTrackerState> SESSION_TRACKER =
            new CoreStateFiles<>( "session-tracker", DUAL, new GlobalSessionTrackerState.Marshal(), CausalClusteringSettings.global_session_tracker_state_size,
                    1 );
    public static final CoreStateFiles<IdAllocationState> ID_ALLOCATION =
            new CoreStateFiles<>( "id-allocation", DUAL, new IdAllocationState.Marshal(), CausalClusteringSettings.id_alloc_state_size, 2 );
    public static final CoreStateFiles<RaftCoreState> RAFT_CORE_STATE = new CoreStateFiles<>( "core", NONE, new RaftCoreState.Marshal(), 3 );

    public static final CoreStateFiles<DatabaseName> DB_NAME = new CoreStateFiles<>( "db-name", SIMPLE, new DatabaseName.Marshal(), 4 );
    public static final CoreStateFiles<ClusterId> CLUSTER_ID = new CoreStateFiles<>( "cluster-id", SIMPLE, new ClusterId.Marshal(), 5 );
    public static final CoreStateFiles<MemberId> CORE_MEMBER_ID = new CoreStateFiles<>( "core-member-id", SIMPLE, new MemberId.Marshal(), 6 );

    //Raft Core state is different from other core state in that you cannot create Storage and marshal raft state to it. However it is still needed in this
    // "enum" in case of migrations.
    public static final CoreStateFiles<RaftLog> RAFT_LOG = new CoreStateFiles<>( "raft-log", SEGMENTED, unsupportedRaftLogMarshal, 7 );
    public static final CoreStateFiles<TermState> RAFT_TERM =
            new CoreStateFiles<>( "term", DUAL, new TermState.Marshal(), CausalClusteringSettings.term_state_size, 8 );
    public static final CoreStateFiles<VoteState> RAFT_VOTE =
            new CoreStateFiles<>( "vote", DUAL, new VoteState.Marshal(), CausalClusteringSettings.vote_state_size, 9 );
    public static final CoreStateFiles<RaftMembershipState> RAFT_MEMBERSHIP =
            new CoreStateFiles<>( "membership", DUAL, new RaftMembershipState.Marshal(), CausalClusteringSettings.raft_membership_state_size, 10 );

    public static final CoreStateFiles<Long> LAST_FLUSHED =
            new CoreStateFiles<>( "last-flushed", DUAL, new LongIndexMarshal(), CausalClusteringSettings.last_flushed_state_size, 11 );

    // for testing purposes
    public static <S> CoreStateFiles<S> DUMMY( SafeStateMarshal<S> marshal )
    {
        return new CoreStateFiles<>( "dummy", SIMPLE, marshal, -1 );
    }

    private static List<CoreStateFiles<?>> values;

    static
    {
        values = Arrays.asList( ID_ALLOCATION, LOCK_TOKEN, DB_NAME, CLUSTER_ID, CORE_MEMBER_ID, RAFT_LOG, RAFT_TERM, RAFT_VOTE, RAFT_MEMBERSHIP,
                RAFT_CORE_STATE, LAST_FLUSHED, SESSION_TRACKER );
        values.sort( Comparator.comparingInt( a -> a.ordinal ) );
        values = Collections.unmodifiableList( values );
    }

    public static List<CoreStateFiles<?>> values()
    {
        return values;
    }

    private final String directoryBaseName;
    private final FileType fileType;
    private final SafeStateMarshal<STATE> marshal;
    private final Setting<Integer> rotationSizeSetting;
    private final int ordinal;

    private CoreStateFiles( String directoryBaseName, FileType fileType, SafeStateMarshal<STATE> marshal, int ordinal )
    {
        this( directoryBaseName, fileType, marshal, null, ordinal );
    }

    private CoreStateFiles( String directoryBaseName, FileType fileType, SafeStateMarshal<STATE> marshal, Setting<Integer> rotationSizeSetting, int ordinal )
    {
        this.directoryBaseName = directoryBaseName;
        this.fileType = fileType;
        this.marshal = marshal;
        this.ordinal = ordinal;
        this.rotationSizeSetting = rotationSizeSetting;
    }

    public File at( File root )
    {
        return new File( root, fileType.dir.apply( directoryBaseName ) );
    }

    public String directoryBaseName()
    {
        return directoryBaseName;
    }

    public String directoryFullName()
    {
        return fileType.dir.apply( directoryBaseName );
    }

    public FileType fileType()
    {
        return fileType;
    }

    public int rotationSize( Config config )
    {
        if ( rotationSizeSetting == null )
        {
            throw new UnsupportedOperationException( "Simple Core state does not rotate and so has no rotation size setting!" );
        }
        return config.get( rotationSizeSetting );
    }

    public SafeStateMarshal<STATE> marshal()
    {
        return marshal;
    }

    public int ordinal()
    {
        return ordinal;
    }

    @Override
    public String toString()
    {
        return directoryFullName();
    }
}
