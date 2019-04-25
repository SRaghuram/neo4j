/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.consensus.log.RaftLog;
import com.neo4j.causalclustering.core.consensus.membership.RaftMembershipState;
import com.neo4j.causalclustering.core.consensus.term.TermState;
import com.neo4j.causalclustering.core.consensus.vote.VoteState;
import com.neo4j.causalclustering.core.replication.session.GlobalSessionTrackerState;
import com.neo4j.causalclustering.core.state.machines.id.IdAllocationState;
import com.neo4j.causalclustering.core.state.machines.locks.ReplicatedLockTokenState;
import com.neo4j.causalclustering.core.state.snapshot.RaftCoreState;
import com.neo4j.causalclustering.core.state.storage.SafeStateMarshal;
import com.neo4j.causalclustering.identity.ClusterId;
import com.neo4j.causalclustering.identity.MemberId;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.config.Setting;

import static com.neo4j.causalclustering.core.CausalClusteringSettings.global_session_tracker_state_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.id_alloc_state_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.last_flushed_state_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.raft_membership_state_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.replicated_lock_token_state_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.term_state_size;
import static com.neo4j.causalclustering.core.CausalClusteringSettings.vote_state_size;
import static com.neo4j.causalclustering.core.state.CoreStateFiles.Scope.DATABASE;
import static com.neo4j.causalclustering.core.state.CoreStateFiles.Scope.GLOBAL;
import static java.util.Arrays.asList;

/**
 * Enumerates and categorises core cluster state files.
 *
 * @param <STATE> The state type.
 */
@SuppressWarnings( {"WeakerAccess", "deprecation"} )
public class CoreStateFiles<STATE>
{
    enum Scope
    {
        GLOBAL,
        DATABASE
    }

    public static final CoreStateFiles<ReplicatedLockTokenState> LOCK_TOKEN =
            new CoreStateFiles<>( "lock-token", DATABASE, new ReplicatedLockTokenState.Marshal(), replicated_lock_token_state_size, CoreStateType.LOCK_TOKEN );
    public static final CoreStateFiles<GlobalSessionTrackerState> SESSION_TRACKER =
            new CoreStateFiles<>( "session-tracker", DATABASE, new GlobalSessionTrackerState.Marshal(), global_session_tracker_state_size,
                    CoreStateType.SESSION_TRACKER );
    public static final CoreStateFiles<IdAllocationState> ID_ALLOCATION =
            new CoreStateFiles<>( "id-allocation", DATABASE, new IdAllocationState.Marshal(), id_alloc_state_size, CoreStateType.ID_ALLOCATION );
    public static final CoreStateFiles<RaftCoreState> RAFT_CORE_STATE =
            new CoreStateFiles<>( "core", DATABASE, new RaftCoreState.Marshal(), CoreStateType.RAFT_CORE_STATE );
    public static final CoreStateFiles<ClusterId> CLUSTER_ID = new CoreStateFiles<>( "cluster-id", GLOBAL, new ClusterId.Marshal(), CoreStateType.CLUSTER_ID );
    public static final CoreStateFiles<MemberId> CORE_MEMBER_ID =
            new CoreStateFiles<>( "core-member-id", GLOBAL, new MemberId.Marshal(), CoreStateType.CORE_MEMBER_ID );
    public static final CoreStateFiles<RaftLog> RAFT_LOG = new CoreStateFiles<>( "raft-log", DATABASE, null, CoreStateType.RAFT_LOG );
    public static final CoreStateFiles<TermState> RAFT_TERM =
            new CoreStateFiles<>( "term", DATABASE, new TermState.Marshal(), term_state_size, CoreStateType.RAFT_TERM );
    public static final CoreStateFiles<VoteState> RAFT_VOTE =
            new CoreStateFiles<>( "vote", DATABASE, new VoteState.Marshal(), vote_state_size, CoreStateType.RAFT_VOTE );
    public static final CoreStateFiles<RaftMembershipState> RAFT_MEMBERSHIP =
            new CoreStateFiles<>( "membership", DATABASE, new RaftMembershipState.Marshal(), raft_membership_state_size, CoreStateType.RAFT_MEMBERSHIP );
    public static final CoreStateFiles<Long> LAST_FLUSHED =
            new CoreStateFiles<>( "last-flushed", DATABASE, new LongIndexMarshal(), last_flushed_state_size, CoreStateType.LAST_FLUSHED );

    // for testing purposes
    public static <S> CoreStateFiles<S> DUMMY( SafeStateMarshal<S> marshal )
    {
        return new CoreStateFiles<>( "dummy", DATABASE, marshal, CoreStateType.DUMMY );
    }

    private static List<CoreStateFiles<?>> values;

    static
    {
        values = asList( ID_ALLOCATION, LOCK_TOKEN, CLUSTER_ID, CORE_MEMBER_ID, RAFT_LOG, RAFT_TERM, RAFT_VOTE, RAFT_MEMBERSHIP, RAFT_CORE_STATE,
                LAST_FLUSHED, SESSION_TRACKER );
        values.sort( Comparator.comparingInt( CoreStateFiles::typeId ) );
        values = Collections.unmodifiableList( values );
    }

    public static List<CoreStateFiles<?>> values()
    {
        return values;
    }

    private final String name;
    private final Scope scope;
    private final SafeStateMarshal<STATE> marshal;
    private final Setting<Integer> rotationSizeSetting;
    private final CoreStateType typeId;

    private CoreStateFiles( String name, Scope scope, SafeStateMarshal<STATE> marshal, CoreStateType typeId )
    {
        this( name, scope, marshal, null, typeId );
    }

    private CoreStateFiles( String name, Scope scope, SafeStateMarshal<STATE> marshal, Setting<Integer> rotationSizeSetting, CoreStateType typeId )
    {
        this.name = name;
        this.scope = scope;
        this.marshal = marshal;
        this.typeId = typeId;
        this.rotationSizeSetting = rotationSizeSetting;
    }

    public String name()
    {
        return name;
    }

    public Scope scope()
    {
        return scope;
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
        return name();
    }
}
