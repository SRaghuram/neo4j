/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.barrier;

import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.consensus.NoLeaderFoundException;
import com.neo4j.causalclustering.core.replication.ReplicationFailureException;
import com.neo4j.causalclustering.core.replication.Replicator;
import com.neo4j.causalclustering.core.state.Result;
import com.neo4j.causalclustering.identity.MemberId;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.lock.AcquireLockTimeoutException;

import static org.neo4j.kernel.api.exceptions.Status.Cluster.NoLeaderAvailable;
import static org.neo4j.kernel.api.exceptions.Status.Cluster.NotALeader;
import static org.neo4j.kernel.api.exceptions.Status.Cluster.ReplicationFailure;

public class BarrierState
{
    public static final String TOKEN_NOT_ON_LEADER_ERROR_MESSAGE = "Should only attempt to take token when leader.";

    private int barrierTokenId = BarrierToken.INVALID_BARRIER_TOKEN_ID;
    private final MemberId myself;
    private final Replicator replicator;
    private final LeaderLocator leaderLocator;
    private final ReplicatedBarrierTokenStateMachine barrierTokenStateMachine;
    private final DatabaseId databaseId;

    public BarrierState( MemberId myself, Replicator replicator, LeaderLocator leaderLocator,
                         ReplicatedBarrierTokenStateMachine barrierTokenStateMachine, DatabaseId databaseId )
    {
        this.myself = myself;
        this.replicator = replicator;
        this.leaderLocator = leaderLocator;
        this.barrierTokenStateMachine = barrierTokenStateMachine;
        this.databaseId = databaseId;
    }

    /**
     * This ensures that a valid token was held at some point in time. It throws an
     * exception if it was held but was later lost or never could be taken to
     * begin with.
     */
    public void ensureHoldingToken()
    {
        if ( barrierTokenId == BarrierToken.INVALID_BARRIER_TOKEN_ID )
        {
            barrierTokenId = acquireTokenOrThrow();
        }
        else if ( barrierTokenId != currentToken().id() )
        {
            throw new AcquireLockTimeoutException( "Local instance lost lock token.", NotALeader );
        }
    }

    public int getCurrentToken()
    {
        return barrierTokenId;
    }

    private BarrierToken currentToken()
    {
        ReplicatedBarrierTokenState state = barrierTokenStateMachine.snapshot();
        return new ReplicatedBarrierTokenRequest( state, databaseId );
    }

    /**
     * Acquires a valid token id owned by us or throws.
     */
    private synchronized int acquireTokenOrThrow()
    {
        BarrierToken currentToken = currentToken();
        if ( myself.equals( currentToken.owner() ) )
        {
            return currentToken.id();
        }

        /* If we are not the leader then we will not even attempt to get the token,
           since only the leader should take locks. */
        ensureLeader();

        ReplicatedBarrierTokenRequest lockTokenRequest =
                new ReplicatedBarrierTokenRequest( myself, BarrierToken.nextCandidateId( currentToken.id() ), databaseId );

        Result result;
        try
        {
            result = replicator.replicate( lockTokenRequest );
        }
        catch ( ReplicationFailureException e )
        {
            throw new AcquireLockTimeoutException( e, "Replication failure acquiring lock token.", ReplicationFailure );
        }

        try
        {
            boolean success = (boolean) result.consume();
            if ( success )
            {
                return lockTokenRequest.id();
            }
            else
            {
                throw new AcquireLockTimeoutException( "Failed to acquire lock token. Was taken by another candidate.",
                        NotALeader );
            }
        }
        catch ( Exception e )
        {
            throw new AcquireLockTimeoutException( e, "Failed to acquire lock token.", NotALeader );
        }
    }

    private void ensureLeader()
    {
        MemberId leader;

        try
        {
            leader = leaderLocator.getLeader();
        }
        catch ( NoLeaderFoundException e )
        {
            throw new AcquireLockTimeoutException( e, "Could not acquire lock token.", NoLeaderAvailable );
        }

        if ( !leader.equals( myself ) )
        {
            throw new AcquireLockTimeoutException( TOKEN_NOT_ON_LEADER_ERROR_MESSAGE, NotALeader );
        }
    }
}
