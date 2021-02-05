/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.lease;

import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionStateMachine;

import java.util.stream.Stream;

import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.AcquireLockTimeoutException;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceType;

/**
 * Each member of the cluster uses its own {@link LeaderOnlyLockManager} which wraps a local {@link Locks} manager.
 * The validity of local lock managers is synchronized by using a lease which gets requested by each server as necessary
 * and if the request is granted then the associated id can be used to demarcate a unique lease period in the cluster.
 * <p/>
 * The fundamental strategy is to only allow locks on the leader. This has the benefit of minimizing the synchronization
 * to only concern the single lease but it also means that non-leaders should not even attempt to request a lease or
 * significant churn of this single resource will lead to a high level of aborted transactions.
 * <p/>
 * The lease carries an id and transactions get ordered with respect to it in the consensus machinery.
 *
 * The latest request which gets accepted (see {@link ReplicatedTransactionStateMachine}) defines the currently valid
 * lease id in this ordering. Each transaction that uses locking gets marked with the lease that was
 * valid at the time of acquiring it, but by the time a transaction is about to be committed it might no longer be valid,
 * which in such case would lead to the transaction being aborted.
 * <p/>
 * The {@link ReplicatedLeaseStateMachine} handles the lease requests and considers only one to be valid at a time.
 * Meanwhile, {@link ReplicatedTransactionStateMachine} rejects any transactions that get committed under an
 * lease.
 */

// TODO: Fix lock exception usage when lock exception hierarchy has been fixed.
public class LeaderOnlyLockManager implements Locks
{
    private final Locks localLocks;

    public LeaderOnlyLockManager( Locks localLocks )
    {
        this.localLocks = localLocks;
    }

    @Override
    public Locks.Client newClient()
    {
        return new LeaderOnlyLockClient( localLocks.newClient() );
    }

    @Override
    public void accept( Visitor visitor )
    {
        localLocks.accept( visitor );
    }

    @Override
    public void close()
    {
        localLocks.close();
    }

    /**
     * The LeaderOnlyLockClient delegates to a local lock client for taking locks, but makes
     * sure that it holds the cluster lease before actually taking locks. If the lease
     * is lost during a locking session then a transaction will either fail on a subsequent
     * local locking operation or during commit time.
     */
    private static class LeaderOnlyLockClient implements Locks.Client
    {
        private final Client localClient;
        private LeaseClient leaseClient;

        LeaderOnlyLockClient( Client localClient )
        {
            this.localClient = localClient;
        }

        @Override
        public void initialize( LeaseClient leaseClient, long transactionId )
        {
            this.leaseClient = leaseClient;
        }

        @Override
        public void acquireShared( LockTracer tracer, ResourceType resourceType, long... resourceId ) throws AcquireLockTimeoutException
        {
            localClient.acquireShared( tracer, resourceType, resourceId );
        }

        @Override
        public void acquireExclusive( LockTracer tracer, ResourceType resourceType, long... resourceId ) throws AcquireLockTimeoutException
        {
            ensureExclusiveLockCanBeAcquired();
            localClient.acquireExclusive( tracer, resourceType, resourceId );
        }

        @Override
        public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
        {
            ensureExclusiveLockCanBeAcquired();
            return localClient.tryExclusiveLock( resourceType, resourceId );
        }

        @Override
        public boolean trySharedLock( ResourceType resourceType, long resourceId )
        {
            return localClient.trySharedLock( resourceType, resourceId );
        }

        @Override
        public boolean reEnterShared( ResourceType resourceType, long resourceId )
        {
            return localClient.reEnterShared( resourceType, resourceId );
        }

        @Override
        public boolean reEnterExclusive( ResourceType resourceType, long resourceId )
        {
            ensureExclusiveLockCanBeAcquired();
            return localClient.reEnterExclusive( resourceType, resourceId );
        }

        @Override
        public void releaseShared( ResourceType resourceType, long... resourceIds )
        {
            localClient.releaseShared( resourceType, resourceIds );
        }

        @Override
        public void releaseExclusive( ResourceType resourceType, long... resourceIds )
        {
            localClient.releaseExclusive( resourceType, resourceIds );
        }

        @Override
        public void prepareForCommit()
        {
            localClient.prepareForCommit();
        }

        @Override
        public void stop()
        {
            localClient.stop();
        }

        @Override
        public void close()
        {
            localClient.close();
        }

        @Override
        public int getLockSessionId()
        {
            return leaseClient.leaseId();
        }

        @Override
        public Stream<ActiveLock> activeLocks()
        {
            return localClient.activeLocks();
        }

        @Override
        public long activeLockCount()
        {
            return localClient.activeLockCount();
        }

        void ensureExclusiveLockCanBeAcquired()
        {
            leaseClient.ensureValid();
        }
    }
}
