package com.neo4j.kernel.impl.enterprise.lock.forseti;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.ThreadLocalRandom;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.impl.api.LeaseService;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.memory.GlobalMemoryGroupTracker;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryGroup;
import org.neo4j.memory.MemoryPools;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.Race;
import org.neo4j.time.Clocks;

import static org.assertj.core.api.Assertions.assertThat;

public class ForsetiMemoryTrackingTest
{
    private GlobalMemoryGroupTracker memoryPool;
    private MemoryTracker memoryTracker;
    private ForsetiLockManager forsetiLockManager;

    @BeforeEach
    void setUp()
    {
        memoryPool = new MemoryPools().pool( MemoryGroup.TRANSACTION, 0L, null );
        memoryTracker = new LocalMemoryTracker( memoryPool );
        forsetiLockManager = new ForsetiLockManager( Config.defaults(), Clocks.nanoClock(), ResourceTypes.values() );
    }

    @AfterEach
    void tearDown()
    {
        assertThat( memoryTracker.estimatedHeapMemory() ).isEqualTo( 0 );
        memoryTracker.close();
        assertThat( memoryPool.getPoolMemoryTracker().estimatedHeapMemory() ).isEqualTo( 0 );
    }

    @Test
    void trackSomeMemory()
    {
        Locks.Client client = getClient();
        client.acquireShared( LockTracer.NONE, ResourceTypes.NODE, 1 );
        client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 2 );
        assertThat( memoryTracker.estimatedHeapMemory() ).isGreaterThan( 0 );
        client.close();
    }

    @Test
    void releaseMemoryOnUnlock()
    {
        Locks.Client client = getClient();
        client.acquireShared( LockTracer.NONE, ResourceTypes.NODE, 1 );
        client.releaseShared( ResourceTypes.NODE, 1 );
        client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 2 );
        long lockedSize = memoryTracker.estimatedHeapMemory();
        assertThat( lockedSize ).isGreaterThan( 0 );
        client.releaseExclusive( ResourceTypes.NODE, 2 );
        assertThat( memoryTracker.estimatedHeapMemory() ).isLessThan( lockedSize );
        client.close();
    }

    @Test
    void upgradingLockShouldNotLeakMemory()
    {
        Locks.Client client = getClient();
        client.acquireShared( LockTracer.NONE, ResourceTypes.NODE, 1 );
        client.acquireShared( LockTracer.NONE, ResourceTypes.NODE, 1 );
        client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 1 ); // Should be upgraded
        client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 1 );
        client.releaseExclusive( ResourceTypes.NODE, 1 );
        client.releaseExclusive( ResourceTypes.NODE, 1 );
        client.releaseShared( ResourceTypes.NODE, 1 );
        client.releaseShared( ResourceTypes.NODE, 1 );
        client.close();
    }

    @Test
    void closeShouldReleaseAllMemory()
    {
        Locks.Client client = getClient();
        client.acquireShared( LockTracer.NONE, ResourceTypes.NODE, 1 );
        client.acquireShared( LockTracer.NONE, ResourceTypes.NODE, 1 );
        client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 1 ); // Should be upgraded
        client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 1 );
        client.close();
    }

    @Test
    void concurrentMemoryShouldEndUpZero() throws Throwable
    {
        Race race = new Race();
        int numThreads = 100;
        LocalMemoryTracker[] trackers = new LocalMemoryTracker[numThreads];
        for ( int i = 0; i < numThreads; i++ )
        {
            trackers[i] = new LocalMemoryTracker( memoryPool );
            Locks.Client client = forsetiLockManager.newClient();
            client.initialize( LeaseService.NoLeaseClient.INSTANCE, 1, trackers[i] );
            race.addContestant( new SimulatedTransaction( client ) );
        }
        race.go();
        for ( int i = 0; i < numThreads; i++ )
        {
            LocalMemoryTracker tracker = trackers[i];
            assertThat( tracker.estimatedHeapMemory() ).isGreaterThanOrEqualTo( 0 );
            tracker.close();
        }
    }

    private static class SimulatedTransaction implements Runnable
    {
        private final Deque<LockEvent> heldLocks = new ArrayDeque<>();
        private final Locks.Client client;

        SimulatedTransaction( Locks.Client client )
        {
            this.client = client;
        }

        @Override
        public void run()
        {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            try
            {
                for ( int i = 0; i < 100; i++ )
                {
                    if ( heldLocks.isEmpty() || random.nextFloat() > 0.33 )
                    {
                        // Acquire new lock
                        int nodeId = random.nextInt( 10 );
                        if ( random.nextBoolean() )
                        {
                            // Exclusive
                            if ( random.nextBoolean() )
                            {
                                client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, nodeId );
                                heldLocks.push( new LockEvent( true, nodeId ) );
                            }
                            else
                            {
                                if ( client.tryExclusiveLock( ResourceTypes.NODE, nodeId ) )
                                {
                                    heldLocks.push( new LockEvent( true, nodeId ) );
                                }
                            }
                        }
                        else
                        {
                            // Shared
                            if ( random.nextBoolean() )
                            {
                                client.acquireShared( LockTracer.NONE, ResourceTypes.NODE, nodeId );
                                heldLocks.push( new LockEvent( false, nodeId ) );
                            }
                            else
                            {
                                if ( client.trySharedLock( ResourceTypes.NODE, nodeId ) )
                                {
                                    heldLocks.push( new LockEvent( false, nodeId ) );
                                }
                            }
                        }
                    }
                    else
                    {
                        // Release old lock
                        LockEvent pop = heldLocks.pop();
                        if ( pop.isExclusive )
                        {
                            client.releaseExclusive( ResourceTypes.NODE, pop.nodeId );
                        }
                        else
                        {
                            client.releaseShared( ResourceTypes.NODE, pop.nodeId );
                        }
                    }
                }
            }
            catch ( DeadlockDetectedException ignore )
            {
            }
            finally
            {
                client.close(); // Should release all of the locks, end resolve deadlock
            }
        }
        private static class LockEvent
        {
            final boolean isExclusive;
            final long nodeId;
            LockEvent( boolean isExclusive, long nodeId )
            {
                this.isExclusive = isExclusive;
                this.nodeId = nodeId;
            }
        }
    }

    private Locks.Client getClient()
    {
        Locks.Client client = forsetiLockManager.newClient();
        client.initialize( LeaseService.NoLeaseClient.INSTANCE, 1, memoryTracker );
        return client;
    }
}
