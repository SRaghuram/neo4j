/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.procedure.enterprise.builtin;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.api.TestKernelTransactionHandle;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceType;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.resources.CpuClock;
import org.neo4j.time.Clocks;
import org.neo4j.values.virtual.VirtualValues;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.lock.LockType.EXCLUSIVE;
import static org.neo4j.lock.LockType.SHARED;

class TransactionDependenciesResolverTest
{
    @Test
    void detectIndependentTransactionsAsNotBlocked()
    {
        HashMap<KernelTransactionHandle,Optional<QuerySnapshot>> map = new HashMap<>();
        TestKernelTransactionHandle handle1 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction() );
        TestKernelTransactionHandle handle2 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction() );

        map.put( handle1, Optional.of( createQuerySnapshot( 1 ) ) );
        map.put( handle2, Optional.of( createQuerySnapshot( 2 ) ) );
        TransactionDependenciesResolver resolver = new TransactionDependenciesResolver( map );

        assertFalse( resolver.isBlocked( handle1 ) );
        assertFalse( resolver.isBlocked( handle2 ) );
    }

    @Test
    void detectBlockedTransactionsByExclusiveLock()
    {
        HashMap<KernelTransactionHandle,Optional<QuerySnapshot>> map = new HashMap<>();
        TestKernelTransactionHandle handle1 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction(), 0,
                singletonList( ActiveLock.exclusiveLock( ResourceTypes.NODE, 1 ) ) );
        TestKernelTransactionHandle handle2 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction() );

        map.put( handle1, Optional.of( createQuerySnapshot( 1 ) ) );
        map.put( handle2, Optional.of( createQuerySnapshotWaitingForLock( 2, SHARED, ResourceTypes.NODE, 1 ) ) );
        TransactionDependenciesResolver resolver = new TransactionDependenciesResolver( map );

        assertFalse( resolver.isBlocked( handle1 ) );
        assertTrue( resolver.isBlocked( handle2 ) );
    }

    @Test
    void detectBlockedTransactionsBySharedLock()
    {
        HashMap<KernelTransactionHandle,Optional<QuerySnapshot>> map = new HashMap<>();
        TestKernelTransactionHandle handle1 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction(), 0,
                singletonList( ActiveLock.sharedLock( ResourceTypes.NODE, 1 ) ) );
        TestKernelTransactionHandle handle2 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction() );

        map.put( handle1, Optional.of( createQuerySnapshot( 1 ) ) );
        map.put( handle2, Optional.of( createQuerySnapshotWaitingForLock( 2, EXCLUSIVE, ResourceTypes.NODE, 1 ) ) );
        TransactionDependenciesResolver resolver = new TransactionDependenciesResolver( map );

        assertFalse( resolver.isBlocked( handle1 ) );
        assertTrue( resolver.isBlocked( handle2 ) );
    }

    @Test
    void blockingChainDescriptionForIndependentTransactionsIsEmpty()
    {
        HashMap<KernelTransactionHandle,Optional<QuerySnapshot>> map = new HashMap<>();
        TestKernelTransactionHandle handle1 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction() );
        TestKernelTransactionHandle handle2 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction() );

        map.put( handle1, Optional.of( createQuerySnapshot( 1 ) ) );
        map.put( handle2, Optional.of( createQuerySnapshot( 2 ) ) );
        TransactionDependenciesResolver resolver = new TransactionDependenciesResolver( map );

        assertThat( resolver.describeBlockingTransactions( handle1 ) ).isEmpty();
        assertThat( resolver.describeBlockingTransactions( handle2 ) ).isEmpty();
    }

    @Test
    void blockingChainDescriptionForDirectlyBlockedTransaction()
    {
        HashMap<KernelTransactionHandle,Optional<QuerySnapshot>> map = new HashMap<>();
        TestKernelTransactionHandle handle1 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction(), 3,
                singletonList( ActiveLock.exclusiveLock( ResourceTypes.NODE, 1 ) ) );
        TestKernelTransactionHandle handle2 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction() );

        map.put( handle1, Optional.of( createQuerySnapshot( 1 ) ) );
        map.put( handle2, Optional.of( createQuerySnapshotWaitingForLock( 2, SHARED, ResourceTypes.NODE, 1 ) ) );
        TransactionDependenciesResolver resolver = new TransactionDependenciesResolver( map );

        assertThat( resolver.describeBlockingTransactions( handle1 ) ).isEmpty();
        assertEquals( "[transaction-3]", resolver.describeBlockingTransactions( handle2 ) );
    }

    @Test
    void blockingChainDescriptionForChainedBlockedTransaction()
    {
        HashMap<KernelTransactionHandle,Optional<QuerySnapshot>> map = new HashMap<>();
        TestKernelTransactionHandle handle1 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction(), 4,
                singletonList( ActiveLock.exclusiveLock( ResourceTypes.NODE, 1 ) ) );
        TestKernelTransactionHandle handle2 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction(),
                5, singletonList( ActiveLock.sharedLock( ResourceTypes.NODE, 2) ) );
        TestKernelTransactionHandle handle3 = new TestKernelTransactionHandleWithLocks( new StubKernelTransaction(), 6 );

        map.put( handle1, Optional.of( createQuerySnapshot( 1 ) ) );
        map.put( handle2, Optional.of( createQuerySnapshotWaitingForLock( 2, EXCLUSIVE, ResourceTypes.NODE, 1 ) ) );
        map.put( handle3, Optional.of( createQuerySnapshotWaitingForLock( 3, EXCLUSIVE, ResourceTypes.NODE, 2 ) ) );
        TransactionDependenciesResolver resolver = new TransactionDependenciesResolver( map );

        assertThat( resolver.describeBlockingTransactions( handle1 ) ).isEmpty();
        assertEquals( "[transaction-4]", resolver.describeBlockingTransactions( handle2 ) );
        assertEquals( "[transaction-4, transaction-5]", resolver.describeBlockingTransactions( handle3 ) );
    }

    private static QuerySnapshot createQuerySnapshot( long queryId )
    {
        return createExecutingQuery( queryId ).snapshot();
    }

    private static QuerySnapshot createQuerySnapshotWaitingForLock( long queryId, LockType lockType, ResourceType resourceType, long id )
    {
        ExecutingQuery executingQuery = createExecutingQuery( queryId );
        executingQuery.lockTracer().waitForLock( lockType, resourceType, id );
        return executingQuery.snapshot();
    }

    private static ExecutingQuery createExecutingQuery( long queryId )
    {
        return new ExecutingQuery( queryId, ClientConnectionInfo.EMBEDDED_CONNECTION, new TestDatabaseIdRepository().defaultDatabase(), "test", "testQuey",
                VirtualValues.EMPTY_MAP, Collections.emptyMap(), () -> 1L, () -> 1, () -> 2,
                Thread.currentThread().getId(), Thread.currentThread().getName(),
                Clocks.nanoClock(), CpuClock.NOT_AVAILABLE, true );
    }

    private static class TestKernelTransactionHandleWithLocks extends TestKernelTransactionHandle
    {

        private final long userTxId;
        private final List<ActiveLock> locks;

        TestKernelTransactionHandleWithLocks( KernelTransaction tx )
        {
            this( tx, 0, Collections.emptyList() );
        }

        TestKernelTransactionHandleWithLocks( KernelTransaction tx, long userTxId )
        {
            this( tx, userTxId, Collections.emptyList() );
        }

        TestKernelTransactionHandleWithLocks( KernelTransaction tx, long userTxId, List<ActiveLock> locks )
        {
            super( tx );
            this.userTxId = userTxId;
            this.locks = locks;
        }

        @Override
        public Stream<ActiveLock> activeLocks()
        {
            return locks.stream();
        }

        @Override
        public long getUserTransactionId()
        {
            return userTxId;
        }
    }
}
