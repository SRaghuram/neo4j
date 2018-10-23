/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.consistency.newchecker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.NamedThreadFactory;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.kernel.impl.store.RecordStore;

import static java.lang.Long.min;

/**
 * Contains logic for executing checker tasks in parallel.
 */
class ParallelExecution
{
    static final Consumer<Throwable> NOOP_EXCEPTION_HANDLER = t -> {};
    static final int DEFAULT_IDS_PER_CHUNK = 1_000_000;

    private final int numberOfThreads;
    private final Consumer<Throwable> exceptionHandler;
    private int idsPerChunk;

    ParallelExecution( int numberOfThreads, Consumer<Throwable> exceptionHandler, int idsPerChunk )
    {
        this.numberOfThreads = numberOfThreads;
        this.exceptionHandler = exceptionHandler;
        this.idsPerChunk = idsPerChunk;
    }

    void run( String taskName, ThrowingRunnable... runnables ) throws Exception
    {
        var forkJoinPool = Executors.newFixedThreadPool( numberOfThreads, new NamedThreadFactory( getClass().getSimpleName() + "-" + taskName ) );
        try
        {
            Exception exceptionChain = null;
            List<InternalTask> tasks = Arrays.stream( runnables ).map( InternalTask::new ).collect( Collectors.toList() );
            for ( Future<Void> future : forkJoinPool.invokeAll( tasks ) )
            {
                try
                {
                    future.get();
                }
                catch ( Exception e )
                {
                    exceptionChain = Exceptions.chain( exceptionChain, e );
                }
            }
            if ( exceptionChain != null )
            {
                throw exceptionChain;
            }
        }
        finally
        {
            forkJoinPool.shutdown();
        }
    }

    ThrowingRunnable[] partition( RecordStore<?> store, RangeOperation rangeOperation )
    {
        LongRange range = LongRange.range( store.getNumberOfReservedLowIds(), store.getHighId() );
        return partition( range, rangeOperation );
    }

    ThrowingRunnable[] partition( LongRange range, RangeOperation rangeOperation )
    {
        List<ThrowingRunnable> partitions = new ArrayList<>();
        for ( long id = range.from(); id < range.to(); id += idsPerChunk )
        {
            long to = min( id + idsPerChunk, range.to() );
            boolean last = to == range.to();
            partitions.add( rangeOperation.operation( id, to, last ) );
        }
        return partitions.toArray( new ThrowingRunnable[0] );
    }

    int getNumberOfThreads()
    {
        return numberOfThreads;
    }

    interface ThrowingRunnable extends Callable<Void>
    {
        @Override
        default Void call() throws Exception
        {
            doRun();
            return null;
        }

        void doRun() throws Exception;
    }

    private class InternalTask implements Callable<Void>
    {
        private final ThrowingRunnable runnable;
        InternalTask( ThrowingRunnable runnable )
        {
            this.runnable = runnable;
        }

        @Override
        public Void call() throws Exception
        {
            try
            {
                runnable.call();
            }
            catch ( Throwable t )
            {
                exceptionHandler.accept( t );
                throw t;
            }
            return null;
        }
    }

    interface RangeOperation
    {
        ThrowingRunnable operation( long from, long to, boolean last );
    }
}
