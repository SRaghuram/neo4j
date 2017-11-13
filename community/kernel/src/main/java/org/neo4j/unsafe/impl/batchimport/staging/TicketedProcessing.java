/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.unsafe.impl.batchimport.staging;

import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import org.neo4j.concurrent.AsyncApply;
import org.neo4j.concurrent.WorkSync;
import org.neo4j.unsafe.impl.batchimport.Parallelizable;
import org.neo4j.unsafe.impl.batchimport.executor.DynamicTaskExecutor;
import org.neo4j.unsafe.impl.batchimport.executor.ParkStrategy;
import org.neo4j.unsafe.impl.batchimport.executor.TaskExecutor;

import static java.lang.System.nanoTime;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.helpers.FutureAdapter.future;

/**
 * Accepts jobs and processes them, potentially in parallel. Each task is given a ticket, an incrementing
 * integer where results of processed tasks are {@link #next() returned} in the order of ticket,
 * i.e. in order of submission. Number of threads processing jobs can be controlled via methods from
 * {@link Parallelizable}. This little ASCII image will make an attempt to visualize this simple flow.
 *
 * <pre>
 *                      Processors...
 *                        ┌────┐
 *                     ┌─>│Job ├──┐
 *                     │  └────┘  │
 * ┌────────────────┐  │  ┌────┐  │  ┌─────────────────┐
 * │Submitted jobs  ├──┼─>│Job ├──┼─>│Processed jobs   │
 * └────────────────┘  │  └────┘  │  └─────────────────┘
 *                     │  ┌────┐  │
 *                     └─>│Job ├──┘
 *                        └────┘
 * </pre>
 *
 * For easily spawning a thread sitting and submitting jobs to be processed from an {@link Iterator},
 * see {@link #slurp(Iterator, boolean)}.
 *
 * @param <FROM> raw material to process
 * @param <STATE> thread local state that each processing thread will share between jobs
 * @param <TO> result that a raw material will be processed into
 */
public class TicketedProcessing<FROM,STATE,TO> implements Parallelizable, AutoCloseable, Panicable
{
    private static final ParkStrategy park = new ParkStrategy.Park( 10, MILLISECONDS );

    private final String name;
    private final TaskExecutor<LocalState<STATE>> executor;
    private final BiFunction<FROM,STATE,TO> processor;
    private final ArrayBlockingQueue<TO> processed;
    private final AtomicLong submittedTicket = new AtomicLong( -1 );
    private final AtomicLong processedTicket = new AtomicLong( -1 );
    private final Runnable healthCheck;
    private final WorkSync<Downstream,SendDownstream> downstreamWorkSync;
    private final LongAdder idleTime = new LongAdder();
    private volatile boolean done;

    public TicketedProcessing( String name, int maxProcessors, BiFunction<FROM,STATE,TO> processor,
            Supplier<STATE> threadLocalStateSupplier )
    {
        this.name = name;
        this.processor = processor;
        this.executor = new DynamicTaskExecutor<>( 1, maxProcessors, maxProcessors, park, name,
                () -> new LocalState<>( threadLocalStateSupplier.get() ) );
        this.healthCheck = executor::assertHealthy;
        this.processed = new ArrayBlockingQueue<>( maxProcessors );
        this.downstreamWorkSync = new WorkSync<>( new Downstream( processedTicket, this::addToResultQueue ) );
    }

    @SuppressWarnings( "unchecked" )
    private long addToResultQueue( TicketedBatch batch )
    {
        long time = nanoTime();
        try
        {
            while ( !processed.offer( (TO) batch.batch, 10, MILLISECONDS ) )
            {
                healthCheck.run();
            }
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException( e );
        }
        return nanoTime() - time;
    }

    /**
     * Submits a job for processing. Results from processed jobs will be available from {@link #next()}
     * in the order in which they got submitted. This method will queue jobs for processing, but not
     * more than the number of current processors and never more than number of maximum processors
     * given in the constructor; the call will block until queue size goes under this threshold.
     * Blocking will provide push-back of submitting new jobs as to reduce unnecessary memory requirements
     * for jobs that will sit and wait to be processed.
     *
     * @param job to process.
     */
    public void submit( FROM job )
    {
        long ticket = submittedTicket.incrementAndGet();
        executor.submit( threadLocalState ->
        {
            // Process this job (we're now in one of the processing threads)
            TO result = processor.apply( job, threadLocalState.actual );
            AsyncApply async = downstreamWorkSync.applyAsync( new SendDownstream( ticket, result, idleTime ) );
            if ( threadLocalState.apply != null )
            {
                try
                {
                    threadLocalState.apply.await();
                    async.await();
                }
                catch ( ExecutionException e )
                {
                    receivePanic( e.getCause() );
                }
            }
            threadLocalState.apply = async;
        } );
    }

    /**
     * Essentially starting a thread {@link #submit(Object) submitting} a stream of inputs which will
     * each be processed and asynchronically made available in order of processing ticket by later calling
     * {@link #next()}.
     *
     * @param input {@link Iterator} of input to process.
     * @param closeAfterAllSubmitted will call {@link #close()} after all jobs submitted if {@code true}.
     * @return {@link Future} representing the work of submitting the inputs to be processed. When the future
     * is completed all items from the {@code input} {@link Iterator} have been submitted, but some items
     * may still be queued and processed.
     */
    public Future<Void> slurp( Iterator<FROM> input, boolean closeAfterAllSubmitted )
    {
        return future( name + "-slurper", () ->
        {
            try
            {
                while ( input.hasNext() )
                {
                    submit( input.next() );
                }
                if ( closeAfterAllSubmitted )
                {
                    close();
                }
                return null;
            }
            catch ( Throwable e )
            {
                receivePanic( e );
                close();
                throw e;
            }
        } );
    }

    /**
     * Tells this processor that there will be no more submissions and so {@link #next()} will stop blocking,
     * waiting for new processed results.
     */
    @Override
    public void close()
    {
        done = true;
        executor.close();
    }

    @Override
    public void receivePanic( Throwable cause )
    {
        executor.receivePanic( cause );
    }

    /**
     * @return next processed job (blocking call), or {@code null} if all jobs have been processed
     * and {@link #close()} has been called.
     */
    public TO next()
    {
        while ( !done || processedTicket.get() < submittedTicket.get() || !processed.isEmpty()  )
        {
            try
            {
                TO next = processed.poll( 10, MILLISECONDS );
                if ( next != null )
                {
                    return next;
                }
            }
            catch ( InterruptedException e )
            {
                // Someone wants us to abort this thing
                Thread.currentThread().interrupt();
                return null;
            }
            healthCheck.run();
        }

        // Health check here too since slurp may have failed and so if not checking health here then
        // a panic may go by unnoticed and may just look like the end of the stream. Checking health here
        // ensures that any slurp panic bubbles up and eventually spreads.
        healthCheck.run();

        return null;
    }

    @Override
    public int processors( int delta )
    {
        return executor.processors( delta );
    }

    private static class LocalState<STATE>
    {
        final STATE actual;
        AsyncApply apply;

        LocalState( STATE actual )
        {
            this.actual = actual;
        }
    }
}
