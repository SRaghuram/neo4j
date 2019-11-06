/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.kernel.impl.locking;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.kernel.impl.locking.LockClientStoppedException;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.lock.AcquireLockTimeoutException;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceType;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static java.lang.Math.abs;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith( RandomExtension.class )
class DeferringLockClientTest
{
    @Inject
    private RandomRule random;

    @Test
    void releaseOfNotHeldSharedLockThrows()
    {
        // GIVEN
        TestLocks actualLocks = new TestLocks();
        TestLocksClient actualClient = actualLocks.newClient();
        DeferringLockClient client = new DeferringLockClient( actualClient );

        assertThrows( IllegalStateException.class, () -> client.releaseShared( ResourceTypes.NODE, 42 ) );

    }

    @Test
    void releaseOfNotHeldExclusiveLockThrows()
    {
        // GIVEN
        TestLocks actualLocks = new TestLocks();
        TestLocksClient actualClient = actualLocks.newClient();
        DeferringLockClient client = new DeferringLockClient( actualClient );

        assertThrows( IllegalStateException.class, () -> client.releaseExclusive( ResourceTypes.NODE, 42 ) );
    }

    @Test
    void shouldDeferAllLocks()
    {
        // GIVEN
        TestLocks actualLocks = new TestLocks();
        TestLocksClient actualClient = actualLocks.newClient();
        DeferringLockClient client = new DeferringLockClient( actualClient );

        // WHEN
        Set<LockUnit> expected = new HashSet<>();
        ResourceType[] types = ResourceTypes.values();
        for ( int i = 0; i < 10_000; i++ )
        {
            boolean exclusive = random.nextBoolean();
            LockUnit lockUnit = new LockUnit( random.among( types ), abs( random.nextLong() ), exclusive );

            if ( exclusive )
            {
                client.acquireExclusive( LockTracer.NONE, lockUnit.resourceType(), lockUnit.resourceId() );
            }
            else
            {
                client.acquireShared( LockTracer.NONE, lockUnit.resourceType(), lockUnit.resourceId() );
            }
            expected.add( lockUnit );
        }
        actualClient.assertRegisteredLocks( Collections.emptySet() );
        client.acquireDeferredLocks( LockTracer.NONE );

        // THEN
        actualClient.assertRegisteredLocks( expected );
    }

    @Test
    void shouldStopUnderlyingClient()
    {
        // GIVEN
        Locks.Client actualClient = mock( Locks.Client.class );
        DeferringLockClient client = new DeferringLockClient( actualClient );

        // WHEN
        client.stop();

        // THEN
        verify( actualClient ).stop();
    }

    @Test
    void shouldPrepareUnderlyingClient()
    {
        // GIVEN
        Locks.Client actualClient = mock( Locks.Client.class );
        DeferringLockClient client = new DeferringLockClient( actualClient );

        // WHEN
        client.prepare();

        // THEN
        verify( actualClient ).prepare();
    }

    @Test
    void shouldCloseUnderlyingClient()
    {
        // GIVEN
        Locks.Client actualClient = mock( Locks.Client.class );
        DeferringLockClient client = new DeferringLockClient( actualClient );

        // WHEN
        client.close();

        // THEN
        verify( actualClient ).close();
    }

    @Test
    void shouldThrowOnAcquireWhenStopped()
    {
        // GIVEN
        Locks.Client actualClient = mock( Locks.Client.class );
        DeferringLockClient client = new DeferringLockClient( actualClient );

        client.stop();

        assertThrows( LockClientStoppedException.class, () -> client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 1 ) );
    }

    @Test
    void shouldThrowOnAcquireWhenClosed()
    {
        // GIVEN
        Locks.Client actualClient = mock( Locks.Client.class );
        DeferringLockClient client = new DeferringLockClient( actualClient );

        client.close();

        assertThrows( LockClientStoppedException.class, () -> client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 1 ) );
    }

    @Test
    void shouldThrowWhenReleaseNotYetAcquiredExclusive()
    {
        // GIVEN
        Locks.Client actualClient = mock( Locks.Client.class );
        DeferringLockClient client = new DeferringLockClient( actualClient );

        assertThrows( IllegalStateException.class, () -> client.releaseExclusive( ResourceTypes.NODE, 1 ) );
    }

    @Test
    void shouldThrowWhenReleaseNotYetAcquiredShared()
    {
        // GIVEN
        Locks.Client actualClient = mock( Locks.Client.class );
        DeferringLockClient client = new DeferringLockClient( actualClient );

        assertThrows( IllegalStateException.class, () -> client.releaseShared( ResourceTypes.NODE, 1 ) );
    }

    @Test
    void shouldThrowWhenReleaseNotMatchingAcquired()
    {
        // GIVEN
        Locks.Client actualClient = mock( Locks.Client.class );
        DeferringLockClient client = new DeferringLockClient( actualClient );

        client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 1 );

        assertThrows( IllegalStateException.class, () -> client.releaseShared( ResourceTypes.NODE, 1 ) );
    }

    @Test
    void shouldThrowWhenReleasingLockMultipleTimes()
    {
        // GIVEN
        Locks.Client actualClient = mock( Locks.Client.class );
        DeferringLockClient client = new DeferringLockClient( actualClient );

        client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 1 );
        client.releaseExclusive( ResourceTypes.NODE, 1 );

        assertThrows( IllegalStateException.class, () -> client.releaseShared( ResourceTypes.NODE, 1 ) );
    }

    @Test
    void exclusiveLockAcquiredMultipleTimesCanNotBeReleasedAtOnce()
    {
        // GIVEN
        TestLocks actualLocks = new TestLocks();
        TestLocksClient actualClient = actualLocks.newClient();
        DeferringLockClient client = new DeferringLockClient( actualClient );

        client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 1 );
        client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 1 );
        client.releaseExclusive( ResourceTypes.NODE, 1 );

        // WHEN
        client.acquireDeferredLocks( LockTracer.NONE );

        // THEN
        actualClient.assertRegisteredLocks( Collections.singleton( new LockUnit( ResourceTypes.NODE, 1, true ) ) );
    }

    @Test
    void sharedLockAcquiredMultipleTimesCanNotBeReleasedAtOnce()
    {
        // GIVEN
        TestLocks actualLocks = new TestLocks();
        TestLocksClient actualClient = actualLocks.newClient();
        DeferringLockClient client = new DeferringLockClient( actualClient );

        client.acquireShared( LockTracer.NONE, ResourceTypes.NODE, 1 );
        client.acquireShared( LockTracer.NONE, ResourceTypes.NODE, 1 );
        client.releaseShared( ResourceTypes.NODE, 1 );

        // WHEN
        client.acquireDeferredLocks( LockTracer.NONE );

        // THEN
        actualClient.assertRegisteredLocks( Collections.singleton( new LockUnit( ResourceTypes.NODE, 1, false ) ) );
    }

    @Test
    void acquireBothSharedAndExclusiveLockThenReleaseShared()
    {
        // GIVEN
        TestLocks actualLocks = new TestLocks();
        TestLocksClient actualClient = actualLocks.newClient();
        DeferringLockClient client = new DeferringLockClient( actualClient );

        client.acquireShared( LockTracer.NONE, ResourceTypes.NODE, 1 );
        client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 1 );
        client.releaseShared( ResourceTypes.NODE, 1 );

        // WHEN
        client.acquireDeferredLocks( LockTracer.NONE );

        // THEN
        actualClient.assertRegisteredLocks( Collections.singleton( new LockUnit( ResourceTypes.NODE, 1, true ) ) );
    }

    @Test
    void exclusiveLocksAcquiredFirst()
    {
        // GIVEN
        TestLocks actualLocks = new TestLocks();
        TestLocksClient actualClient = actualLocks.newClient();
        DeferringLockClient client = new DeferringLockClient( actualClient );

        client.acquireShared( LockTracer.NONE, ResourceTypes.NODE, 1 );
        client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 2 );
        client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 3 );
        client.acquireExclusive( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 1 );
        client.acquireShared( LockTracer.NONE, ResourceTypes.RELATIONSHIP, 2 );
        client.acquireShared( LockTracer.NONE, ResourceTypes.LABEL, 1 );
        client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 42 );

        // WHEN
        client.acquireDeferredLocks( LockTracer.NONE );

        // THEN
        Set<LockUnit> expectedLocks = new LinkedHashSet<>(
                Arrays.asList( new LockUnit( ResourceTypes.NODE, 2, true ),
                        new LockUnit( ResourceTypes.NODE, 3, true ),
                        new LockUnit( ResourceTypes.NODE, 42, true ),
                        new LockUnit( ResourceTypes.RELATIONSHIP, 1, true ),
                        new LockUnit( ResourceTypes.NODE, 1, false ),
                        new LockUnit( ResourceTypes.RELATIONSHIP, 2, false ),
                        new LockUnit( ResourceTypes.LABEL, 1, false ) )
        );

        actualClient.assertRegisteredLocks( expectedLocks );
    }

    @Test
    void acquireBothSharedAndExclusiveLockThenReleaseExclusive()
    {
        // GIVEN
        TestLocks actualLocks = new TestLocks();
        TestLocksClient actualClient = actualLocks.newClient();
        DeferringLockClient client = new DeferringLockClient( actualClient );

        client.acquireShared( LockTracer.NONE, ResourceTypes.NODE, 1 );
        client.acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 1 );
        client.releaseExclusive( ResourceTypes.NODE, 1 );

        // WHEN
        client.acquireDeferredLocks( LockTracer.NONE );

        // THEN
        actualClient.assertRegisteredLocks( Collections.singleton( new LockUnit( ResourceTypes.NODE, 1, false ) ) );
    }

    private static class TestLocks extends LifecycleAdapter implements Locks
    {
        @Override
        public TestLocksClient newClient()
        {
            return new TestLocksClient();
        }

        @Override
        public void accept( Visitor visitor )
        {
        }

        @Override
        public void close()
        {
        }
    }

    private static class TestLocksClient implements Locks.Client
    {
        private final Set<LockUnit> actualLockUnits = new LinkedHashSet<>();

        @Override
        public void initialize( LeaseClient leaseClient )
        {
        }

        @Override
        public void acquireShared( LockTracer tracer, ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
        {
            register( resourceType, false, resourceIds );
        }

        void assertRegisteredLocks( Set<LockUnit> expectedLocks )
        {
            assertEquals( expectedLocks, actualLockUnits );
        }

        private boolean register( ResourceType resourceType, boolean exclusive, long... resourceIds )
        {
            for ( long resourceId : resourceIds )
            {
                actualLockUnits.add( new LockUnit( resourceType, resourceId, exclusive ) );
            }
            return true;
        }

        @Override
        public void acquireExclusive( LockTracer tracer, ResourceType resourceType, long... resourceIds )
                throws AcquireLockTimeoutException
        {
            register( resourceType, true, resourceIds );
        }

        @Override
        public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
        {
            return register( resourceType, true, resourceId );
        }

        @Override
        public boolean trySharedLock( ResourceType resourceType, long resourceId )
        {
            return register( resourceType, false, resourceId );
        }

        @Override
        public boolean reEnterShared( ResourceType resourceType, long resourceId )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean reEnterExclusive( ResourceType resourceType, long resourceId )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void releaseShared( ResourceType resourceType, long... resourceIds )
        {
        }

        @Override
        public void releaseExclusive( ResourceType resourceType, long... resourceIds )
        {
        }

        @Override
        public void prepare()
        {
        }

        @Override
        public void stop()
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        public int getLockSessionId()
        {
            return 0;
        }

        @Override
        public Stream<ActiveLock> activeLocks()
        {
            return actualLockUnits.stream().map( ActiveLock.class::cast );
        }

        @Override
        public long activeLockCount()
        {
            return actualLockUnits.size();
        }
    }
}
