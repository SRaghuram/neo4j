/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.procedure.commercial.builtin;

import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.time.ZoneId;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import org.neo4j.collection.pool.Pool;
import org.neo4j.configuration.Config;
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.api.query.QuerySnapshot;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.api.SchemaState;
import org.neo4j.kernel.impl.api.SchemaWriteGuard;
import org.neo4j.kernel.impl.api.StatementOperationParts;
import org.neo4j.kernel.impl.api.TestKernelTransactionHandle;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionExecutionStatistic;
import org.neo4j.kernel.impl.api.TransactionHooks;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.transaction.trace.TransactionInitializationTrace;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.locking.ActiveLock;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.query.clientconnection.HttpConnectionInfo;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.kernel.impl.util.DefaultValueMapper;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.lock.LockTracer;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier.ON_HEAP;
import static org.neo4j.test.rule.DatabaseRule.mockedTokenHolders;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

class TransactionStatusResultTest
{

    private TestKernelTransactionHandle transactionHandle = new TransactionHandleWithLocks( new StubKernelTransaction() );
    private final HashMap<KernelTransactionHandle,List<QuerySnapshot>> snapshotsMap = new HashMap<>();
    private final TransactionDependenciesResolver blockerResolver = new TransactionDependenciesResolver( snapshotsMap );

    @Test
    void statusOfTransactionWithSingleQuery() throws InvalidArgumentsException
    {
        snapshotsMap.put( transactionHandle, singletonList( createQuerySnapshot( 7L ) ) );
        TransactionStatusResult statusResult =
                new TransactionStatusResult( transactionHandle, blockerResolver, snapshotsMap, ZoneId.of( "UTC" ) );

        checkTransactionStatus( statusResult, "testQuery", "query-7", "1970-01-01T00:00:01.984Z" );
    }

    @Test
    void statusOfTransactionWithoutRunningQuery() throws InvalidArgumentsException
    {
        snapshotsMap.put( transactionHandle, emptyList() );
        TransactionStatusResult statusResult =
                new TransactionStatusResult( transactionHandle, blockerResolver, snapshotsMap, ZoneId.of( "UTC" ) );

        checkTransactionStatusWithoutQueries( statusResult );
    }

    @Test
    void statusOfTransactionWithMultipleQueries() throws InvalidArgumentsException
    {
        snapshotsMap.put( transactionHandle, asList( createQuerySnapshot( 7L ), createQuerySnapshot( 8L ) ) );
        TransactionStatusResult statusResult =
                new TransactionStatusResult( transactionHandle, blockerResolver, snapshotsMap, ZoneId.of( "UTC" ) );

        checkTransactionStatus( statusResult, "testQuery", "query-7", "1970-01-01T00:00:01.984Z" );
    }

    @Test
    void statusOfTransactionWithDifferentTimeZone() throws InvalidArgumentsException
    {
        snapshotsMap.put( transactionHandle, singletonList( createQuerySnapshot( 7L ) ) );
        TransactionStatusResult statusResult =
                new TransactionStatusResult( transactionHandle, blockerResolver, snapshotsMap, ZoneId.of( "UTC+1" ) );

        checkTransactionStatus( statusResult, "testQuery", "query-7", "1970-01-01T01:00:01.984+01:00" );
    }

    @Test
    void emptyInitialisationStacktraceWhenTraceNotAvailable() throws InvalidArgumentsException
    {
        snapshotsMap.put( transactionHandle, emptyList() );
        TransactionStatusResult statusResult = new TransactionStatusResult( transactionHandle, blockerResolver, snapshotsMap, ZoneId.of( "UTC" ) );
        assertEquals( EMPTY, statusResult.initializationStackTrace );
    }

    @Test
    void includeInitialisationStacktraceWhenTraceAvailable() throws InvalidArgumentsException
    {
        transactionHandle = new TransactionHandleWithLocks( new StubKernelTransaction(), true );
        snapshotsMap.put( transactionHandle, emptyList() );
        TransactionStatusResult statusResult = new TransactionStatusResult( transactionHandle, blockerResolver, snapshotsMap, ZoneId.of( "UTC" ) );
        assertThat( statusResult.initializationStackTrace, containsString( "Transaction initialization stacktrace." ) );
    }

    private static void checkTransactionStatusWithoutQueries( TransactionStatusResult statusResult )
    {
        assertEquals( "transaction-8", statusResult.transactionId );
        assertEquals( "testUser", statusResult.username );
        assertEquals( stringObjectEmptyMap(), statusResult.metaData );
        assertEquals( "1970-01-01T00:00:01.984Z", statusResult.startTime );
        assertEquals( "https", statusResult.protocol );
        assertEquals( "https-42", statusResult.connectionId );
        assertEquals( "localhost:1000", statusResult.clientAddress );
        assertEquals( "https://localhost:1001/path", statusResult.requestUri );
        assertEquals( EMPTY, statusResult.currentQueryId );
        assertEquals( EMPTY, statusResult.currentQuery );
        assertEquals( 1, statusResult.activeLockCount );
        assertEquals( "Running", statusResult.status );
        assertEquals( stringObjectEmptyMap(), statusResult.resourceInformation );
        assertEquals( 1810L, statusResult.elapsedTimeMillis );
        assertEquals( Long.valueOf( 1L ), statusResult.cpuTimeMillis );
        assertEquals( 0L, statusResult.waitTimeMillis );
        assertEquals( Long.valueOf( 1809 ), statusResult.idleTimeMillis );
        assertEquals( Long.valueOf( 1 ), statusResult.allocatedBytes );
        assertEquals( Long.valueOf( 0 ), statusResult.allocatedDirectBytes );
        assertEquals( 0L, statusResult.pageHits );
        assertEquals( 0L, statusResult.pageFaults );
    }

    private static void checkTransactionStatus( TransactionStatusResult statusResult, String currentQuery, String currentQueryId, String startTime )
    {
        assertEquals( "transaction-8", statusResult.transactionId );
        assertEquals( "testUser", statusResult.username );
        assertEquals( stringObjectEmptyMap(), statusResult.metaData );
        assertEquals( startTime, statusResult.startTime );
        assertEquals( "https", statusResult.protocol );
        assertEquals( "https-42", statusResult.connectionId );
        assertEquals( "localhost:1000", statusResult.clientAddress );
        assertEquals( "https://localhost:1001/path", statusResult.requestUri );
        assertEquals( currentQueryId, statusResult.currentQueryId );
        assertEquals( currentQuery, statusResult.currentQuery );
        assertEquals( 1, statusResult.activeLockCount );
        assertEquals( "Running", statusResult.status );
        assertEquals( stringObjectEmptyMap(), statusResult.resourceInformation );
        assertEquals( 1810, statusResult.elapsedTimeMillis );
        assertEquals( Long.valueOf( 1 ), statusResult.cpuTimeMillis );
        assertEquals( 0L, statusResult.waitTimeMillis );
        assertEquals( Long.valueOf( 1809 ), statusResult.idleTimeMillis );
        assertEquals( Long.valueOf( 1 ), statusResult.allocatedBytes );
        assertEquals( Long.valueOf( 0 ), statusResult.allocatedDirectBytes );
        assertEquals( 0, statusResult.pageHits );
        assertEquals( 0, statusResult.pageFaults );
    }

    private static Map<String,Object> stringObjectEmptyMap()
    {
        return emptyMap();
    }

    private static QuerySnapshot createQuerySnapshot( long queryId )
    {
        ExecutingQuery executingQuery = createExecutingQuery( queryId );
        return executingQuery.snapshot();
    }

    private static ExecutingQuery createExecutingQuery( long queryId )
    {
        return new ExecutingQuery( queryId, getTestConnectionInfo(), DEFAULT_DATABASE_NAME, "testUser", "testQuery", EMPTY_MAP,
                stringObjectEmptyMap(), () -> 1L, PageCursorTracer.NULL,
                Thread.currentThread().getId(), Thread.currentThread().getName(),
                new CountingNanoClock(), new CountingCpuClock(), new CountingHeapAllocation() );
    }

    private static HttpConnectionInfo getTestConnectionInfo()
    {
        return new HttpConnectionInfo( "https-42", "https", new InetSocketAddress( "localhost", 1000 ),
                new InetSocketAddress( "localhost", 1001 ), "/path" );
    }

    private static class TransactionHandleWithLocks extends TestKernelTransactionHandle
    {
        boolean hasInitTrace;

        TransactionHandleWithLocks( KernelTransaction tx )
        {
            super( tx );
        }

        TransactionHandleWithLocks( KernelTransaction tx, boolean hasInitTrace )
        {
            super( tx );
            this.hasInitTrace = hasInitTrace;
        }

        @Override
        public TransactionInitializationTrace transactionInitialisationTrace()
        {
            if ( hasInitTrace )
            {
                return new TransactionInitializationTrace();
            }
            return super.transactionInitialisationTrace();
        }

        @Override
        public Stream<ActiveLock> activeLocks()
        {
            return Stream.of( ActiveLock.sharedLock( ResourceTypes.NODE, 3 ) );
        }

        @Override
        public TransactionExecutionStatistic transactionStatistic()
        {
            Dependencies dependencies = new Dependencies();
            dependencies.satisfyDependency( mock( DefaultValueMapper.class ) );
            KernelTransactionImplementation transaction = new KernelTransactionImplementation( Config.defaults(),
                        mock( StatementOperationParts.class ), mock( SchemaWriteGuard.class ), new TransactionHooks(),
                        mock( ConstraintIndexCreator.class ), mock( GlobalProcedures.class ), TransactionHeaderInformationFactory.DEFAULT,
                        mock( TransactionCommitProcess.class ), new DatabaseTransactionStats(),
                        mock( Pool.class ), Clocks.fakeClock(),
                        new AtomicReference<>( CpuClock.NOT_AVAILABLE ), new AtomicReference<>( HeapAllocation.NOT_AVAILABLE ),
                        TransactionTracer.NULL,
                        LockTracer.NONE, PageCursorTracerSupplier.NULL,
                        mock( StorageEngine.class, RETURNS_MOCKS ), new CanWrite(),
                        EmptyVersionContextSupplier.EMPTY, ON_HEAP, new StandardConstraintSemantics(), mock( SchemaState.class),
                        mockedTokenHolders(), mock( IndexingService.class ), mock( LabelScanStore.class ),
                        mock( IndexStatisticsStore.class ), dependencies )
            {
                @Override
                public Statistics getStatistics()
                {
                    TestStatistics statistics = new TestStatistics( this, new AtomicReference<>( new CountingCpuClock() ),
                                    new AtomicReference<>( new CountingHeapAllocation() ) );
                    statistics.init( Thread.currentThread().getId(), PageCursorTracer.NULL );
                    return statistics;
                }
            };
            return new TransactionExecutionStatistic( transaction, Clocks.fakeClock().forward( 2010, MILLISECONDS ), 200 );
        }

        @Override
        public ClientConnectionInfo clientInfo()
        {
            return getTestConnectionInfo();
        }
    }

    private static class TestStatistics extends KernelTransactionImplementation.Statistics
    {
        @Override
        protected void init( long threadId, PageCursorTracer pageCursorTracer )
        {
            super.init( threadId, pageCursorTracer );
        }

        TestStatistics( KernelTransactionImplementation transaction, AtomicReference<CpuClock> cpuClockRef,
                AtomicReference<HeapAllocation> heapAllocationRef )
        {
            super( transaction, cpuClockRef, heapAllocationRef );
        }
    }

    private static class CountingNanoClock extends SystemNanoClock
    {
        private long time;

        @Override
        public long nanos()
        {
            time += MILLISECONDS.toNanos( 1 );
            return time;
        }
    }

    private static class CountingCpuClock extends CpuClock
    {
        private long cpuTime;

        @Override
        public long cpuTimeNanos( long threadId )
        {
            cpuTime += MILLISECONDS.toNanos( 1 );
            return cpuTime;
        }
    }

    private static class CountingHeapAllocation extends HeapAllocation
    {
        private long allocatedBytes;

        @Override
        public long allocatedBytes( long threadId )
        {
            return allocatedBytes++;
        }
    }
}
