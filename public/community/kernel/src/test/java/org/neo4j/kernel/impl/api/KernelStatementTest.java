/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api;

import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.query.ExecutingQuery;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.locking.community.CommunityLockClient;
import org.neo4j.lock.LockTracer;
import org.neo4j.resources.CpuClock;
import org.neo4j.time.Clocks;
import org.neo4j.values.virtual.MapValue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KernelStatementTest
{
    private final AtomicReference<CpuClock> cpuClockRef = new AtomicReference<>( CpuClock.NOT_AVAILABLE );

    @Test
    void shouldReleaseResourcesWhenForceClosed()
    {
        // given
        KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
        when( transaction.isSuccess() ).thenReturn( true );
        KernelStatement statement = createStatement( transaction );
        statement.acquire();

        // when
        assertThrows( KernelStatement.StatementNotClosedException.class, statement::forceClose );

        // then
        verify( transaction ).releaseStatementResources();
    }

    @Test
    void assertStatementIsNotOpenWhileAcquireIsNotInvoked()
    {
        KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );
        KernelStatement statement = createStatement( transaction );

        assertThrows( NotInTransactionException.class, statement::assertOpen );
    }

    @Test
    void reportQueryWaitingTimeToTransactionStatisticWhenFinishQueryExecution()
    {
        KernelTransactionImplementation transaction = mock( KernelTransactionImplementation.class );

        KernelTransactionImplementation.Statistics statistics = new KernelTransactionImplementation.Statistics( transaction, cpuClockRef );
        when( transaction.getStatistics() ).thenReturn( statistics );
        when( transaction.executingQuery() ).thenReturn( Optional.empty() );

        KernelStatement statement = createStatement( transaction );
        statement.acquire();

        ExecutingQuery query = getQueryWithWaitingTime();
        ExecutingQuery query2 = getQueryWithWaitingTime();
        ExecutingQuery query3 = getQueryWithWaitingTime();

        statement.stopQueryExecution( query );
        statement.stopQueryExecution( query2 );
        statement.stopQueryExecution( query3 );

        assertEquals( 3, statistics.getWaitingTimeNanos( 1 ) );
    }

    @Test
    void trackSequentialQueriesInStatement()
    {
        var queryFactory = new ExecutingQueryFactory( Clocks.nanoClock(), cpuClockRef, Config.defaults() );
        var transaction = mock( KernelTransactionImplementation.class, RETURNS_DEEP_STUBS );
        var statement = createStatement( transaction );
        statement.initialize( new SimpleStatementLocks( mock( CommunityLockClient.class ) ), PageCursorTracer.NULL, 100 );

        var query1 = queryFactory.createForStatement( statement, "test1", MapValue.EMPTY );
        var query2 = queryFactory.createForStatement( statement, "test2", MapValue.EMPTY );
        var query3 = queryFactory.createForStatement( statement, "test3", MapValue.EMPTY );

        statement.startQueryExecution( query1 );
        statement.startQueryExecution( query2 );
        assertSame( query2, statement.executingQuery().orElseThrow() );

        statement.startQueryExecution( query3 );
        assertSame( query3, statement.executingQuery().orElseThrow() );

        statement.stopQueryExecution( query3 );
        assertSame( query2, statement.executingQuery().orElseThrow() );

        statement.stopQueryExecution( query2 );
        assertSame( query1, statement.executingQuery().orElseThrow() );

        statement.stopQueryExecution( query1 );
        assertFalse( statement.executingQuery().isPresent() );
    }

    private KernelStatement createStatement( KernelTransactionImplementation transaction )
    {
        return new KernelStatement( transaction, LockTracer.NONE, new ClockContext(), EmptyVersionContextSupplier.EMPTY,
                                    cpuClockRef, new TestDatabaseIdRepository().defaultDatabase(), Config.defaults() );
    }

    private static ExecutingQuery getQueryWithWaitingTime()
    {
        ExecutingQuery executingQuery = mock( ExecutingQuery.class );
        when( executingQuery.reportedWaitingTimeNanos() ).thenReturn( 1L );
        return executingQuery;
    }
}
