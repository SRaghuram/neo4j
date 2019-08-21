/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.barrier;

import com.neo4j.causalclustering.core.state.machines.tx.ReplicatedTransactionStateMachine;

import java.util.stream.Stream;

import org.neo4j.kernel.impl.api.Epoch;
import org.neo4j.kernel.impl.api.EpochException;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.AcquireLockTimeoutException;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceType;

/**
 * Each member of the cluster uses its own {@link LeaderOnlyLockManager} which wraps a local {@link Locks} manager.
 * The validity of local lock managers is synchronized by using a token which gets requested by each server as necessary
 * and if the request is granted then the associated id can be used to identify a unique lock session in the cluster.
 * <p/>
 * The fundamental strategy is to only allow locks on the leader. This has the benefit of minimizing the synchronization
 * to only concern the single token but it also means that non-leaders should not even attempt to request the token or
 * significant churn of this single resource will lead to a high level of aborted transactions.
 * <p/>
 * The token requests carry a candidate id and they get ordered with respect to the transactions in the consensus
 * machinery.
 * The latest request which gets accepted (see {@link ReplicatedTransactionStateMachine}) defines the currently valid
 * lock session id in this ordering. Each transaction that uses locking gets marked with a lock session id that was
 * valid
 * at the time of acquiring it, but by the time a transaction commits it might no longer be valid, which in such case
 * would lead to the transaction being rejected and failed.
 * <p/>
 * The {@link ReplicatedBarrierTokenStateMachine} handles the token requests and considers only one to be valid at a time.
 * Meanwhile, {@link ReplicatedTransactionStateMachine} rejects any transactions that get committed under an
 * invalid token.
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
     * sure that it holds the cluster locking token before actually taking locks. If the token
     * is lost during a locking session then a transaction will either fail on a subsequent
     * local locking operation or during commit time.
     */
    private class LeaderOnlyLockClient implements Locks.Client
    {
        private final Client localClient;
        private Epoch epoch;

        LeaderOnlyLockClient( Client localClient )
        {
            this.localClient = localClient;
        }

        @Override
        public void initialize( Epoch epoch )
        {
            this.epoch = epoch;
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
        public void prepare()
        {
            localClient.prepare();
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
            return epoch.tokenId();
        }

        @Override
        public Stream<? extends ActiveLock> activeLocks()
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
            try
            {
                epoch.ensureHoldingToken();
            }
            catch ( EpochException e )
            {
                throw new AcquireLockTimeoutException( "This instance is no longer able to acquire exclusive locks because of leader re-election",
                        e, e.status() );
            }
        }
    }
}
