/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.lease;

import com.neo4j.causalclustering.core.consensus.LeaderLocator;
import com.neo4j.causalclustering.core.replication.ReplicationResult;
import com.neo4j.causalclustering.core.replication.Replicator;
import com.neo4j.causalclustering.identity.RaftMemberId;

import java.util.function.Supplier;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.api.LeaseException;
import org.neo4j.kernel.impl.api.LeaseService;

import static com.neo4j.causalclustering.core.state.machines.lease.Lease.nextCandidateId;
import static org.neo4j.kernel.api.exceptions.Status.Cluster.NotALeader;
import static org.neo4j.kernel.api.exceptions.Status.Cluster.ReplicationFailure;

/**
 * Used for coordinating the acquiring and validation of a lease, both towards other
 * members of the cluster as well as locally under a lease owned by this instance.
 *
 * An lease is acquired for this member using consensus and identified by a unique ID.
 * Within the validity period of a lease only this instance can use the resources protected
 * by it, e.g. locks, record IDs, and conversely the resources coordinated under a lease are
 * invalid outside the lease period.
 */
public class ClusterLeaseCoordinator implements LeaseService
{
    private static final String NOT_ON_LEADER_ERROR_MESSAGE = "Should only attempt to acquire lease when leader.";

    private volatile int invalidLeaseId = NO_LEASE;
    private volatile int myLeaseId = NO_LEASE;

    private final Supplier<RaftMemberId> myself;
    private final Replicator replicator;
    private final LeaderLocator leaderLocator;
    private final ReplicatedLeaseStateMachine leaseStateMachine;
    private final NamedDatabaseId namedDatabaseId;

    public ClusterLeaseCoordinator( Supplier<RaftMemberId> myself, Replicator replicator, LeaderLocator leaderLocator,
            ReplicatedLeaseStateMachine leaseStateMachine, NamedDatabaseId namedDatabaseId )
    {
        this.myself = myself;
        this.replicator = replicator;
        this.leaderLocator = leaderLocator;
        this.leaseStateMachine = leaseStateMachine;
        this.namedDatabaseId = namedDatabaseId;
    }

    @Override
    public LeaseClient newClient()
    {
        return new ClusterLeaseClient( this );
    }

    /**
     * Checks if a previously acquired lease still is valid.
     * Must not be called with {@link LeaseService#NO_LEASE}.
     */
    public boolean isInvalid( int leaseId )
    {
        if ( leaseId == NO_LEASE )
        {
            throw new IllegalArgumentException( "Not a lease" );
        }
        return leaseId == invalidLeaseId || leaseId != leaseStateMachine.leaseId() || leaseId != myLeaseId;
    }

    public synchronized void invalidateLease( int leaseId )
    {
        if ( leaseId != NO_LEASE && leaseId == leaseStateMachine.leaseId() )
        {
            invalidLeaseId = leaseId;
        }
    }

    private Lease currentLease()
    {
        ReplicatedLeaseState state = leaseStateMachine.snapshot();
        return new ReplicatedLeaseRequest( state, namedDatabaseId );
    }

    /**
     * Acquires a valid lease id owned by us or throws.
     */
    synchronized int acquireLeaseOrThrow() throws LeaseException
    {
        Lease currentLease = currentLease();
        if ( myself.get().equals( currentLease.owner() ) && !isInvalid( currentLease.id() ) )
        {
            return currentLease.id();
        }

        /* If we are not the leader then we will not even attempt to acquire the lease. */
        ensureLeader();

        ReplicatedLeaseRequest leaseRequest = new ReplicatedLeaseRequest( myself.get(), nextCandidateId( currentLease.id() ), namedDatabaseId.databaseId() );
        ReplicationResult replicationResult = replicator.replicate( leaseRequest );

        if ( replicationResult.outcome() != ReplicationResult.Outcome.APPLIED )
        {
            throw new LeaseException( "Failed to acquire lease", replicationResult.failure(), ReplicationFailure );
        }

        boolean leaseAcquired;
        try
        {
            leaseAcquired = replicationResult.stateMachineResult().consume();
        }
        catch ( Exception e )
        {
            // the ReplicatedLeaseStateMachine does not produce exceptions as a result
            throw new LeaseException( "Unexpected exception", e, Status.General.UnknownError );
        }

        if ( !leaseAcquired )
        {
            throw new LeaseException( "Failed to acquire lease since it was taken by another candidate", NotALeader );
        }

        invalidLeaseId = currentLease.id();
        return myLeaseId = leaseRequest.id();
    }

    private void ensureLeader() throws LeaseException
    {
        var leaderInfo = leaderLocator.getLeaderInfo();
        RaftMemberId leader = null;
        if ( leaderInfo.isPresent() )
        {
            leader = leaderInfo.get().memberId();
        }
        if ( !myself.get().equals( leader ) )
        {
            throw new LeaseException( NOT_ON_LEADER_ERROR_MESSAGE, NotALeader );
        }
    }
}
