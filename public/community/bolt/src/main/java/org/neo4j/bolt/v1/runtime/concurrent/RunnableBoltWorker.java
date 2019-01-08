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
package org.neo4j.bolt.v1.runtime.concurrent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.neo4j.bolt.v1.runtime.BoltConnectionAuthFatality;
import org.neo4j.bolt.v1.runtime.BoltConnectionFatality;
import org.neo4j.bolt.v1.runtime.BoltProtocolBreachFatality;
import org.neo4j.bolt.v1.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.runtime.BoltWorker;
import org.neo4j.bolt.v1.runtime.BoltWorkerQueueMonitor;
import org.neo4j.bolt.v1.runtime.Job;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

/**
 * Executes incoming Bolt requests for a given connection.
 */
class RunnableBoltWorker implements Runnable, BoltWorker
{
    private static final int workQueueMaxBatchSize = Integer.getInteger( "org.neo4j.bolt.workQueueMaxBatchSize", 100 );
    static final int workQueuePollDuration =  Integer.getInteger( "org.neo4j.bolt.workQueuePollDuration", 10 );

    private final BlockingQueue<Job> jobQueue = new LinkedBlockingQueue<>();
    private final BoltStateMachine machine;
    private final Log log;
    private final Log userLog;
    private final BoltWorkerQueueMonitor queueMonitor;

    private volatile boolean keepRunning = true;

    RunnableBoltWorker( BoltStateMachine machine, LogService logging )
    {
        this( machine, logging, null );
    }

    RunnableBoltWorker( BoltStateMachine machine, LogService logging, BoltWorkerQueueMonitor queueMonitor )
    {
        this.machine = machine;
        this.log = logging.getInternalLog( getClass() );
        this.userLog = logging.getUserLog( getClass() );
        this.queueMonitor = queueMonitor;
    }

    /**
     * Accept a command to be executed at some point in the future. This will get queued and executed as soon as
     * possible.
     *
     * @param job an operation to be performed on the session
     */
    @Override
    public void enqueue( Job job )
    {
        try
        {
            jobQueue.put( job );
            notifyEnqueued( job );
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException( "Worker interrupted while queueing request, the session may have been " +
                                        "forcibly closed, or the database may be shutting down." );
        }
    }

    @Override
    public void run()
    {
        List<Job> batch = new ArrayList<>( workQueueMaxBatchSize );

        try
        {
            while ( keepRunning )
            {
                Job job = jobQueue.poll( workQueuePollDuration, TimeUnit.SECONDS );
                if ( job != null )
                {
                    notifyDequeued( job );
                    execute( job );

                    for ( int jobCount = jobQueue.drainTo( batch, workQueueMaxBatchSize ); keepRunning && jobCount > 0;
                          jobCount = jobQueue.drainTo( batch, workQueueMaxBatchSize ) )
                    {
                        notifyDrained( batch );
                        executeBatch( batch );
                    }
                }
                else
                {
                    machine.validateTransaction();
                }
            }
        }
        catch ( BoltConnectionAuthFatality e )
        {
            // this is logged in the SecurityLog
        }
        catch ( BoltProtocolBreachFatality e )
        {
            log.error( "Bolt protocol breach in session '" + machine.key() + "'", e );
        }
        catch ( Throwable t )
        {
            userLog.error( "Worker for session '" + machine.key() + "' crashed.", t );
        }
        finally
        {
            closeStateMachine();
        }
    }

    private void executeBatch( List<Job> batch ) throws BoltConnectionFatality
    {
        for ( int i = 0; keepRunning && i < batch.size(); i++ )
        {
            execute( batch.get( i ) );
        }
        batch.clear();
    }

    private void execute( Job job ) throws BoltConnectionFatality
    {
        job.perform( machine );
    }

    @Override
    public void interrupt()
    {
        machine.interrupt();
    }

    @Override
    public void halt()
    {
        try
        {
            // Notify the state machine that it should terminate.
            // We can't close it here because this method can be called from a different thread.
            // State machine will be closed when this worker exits.
            machine.terminate();
        }
        finally
        {
            keepRunning = false;
        }
    }

    private void closeStateMachine()
    {
        try
        {
            // Attempt to close the state machine, as an effort to release locks and other resources
            machine.close();
        }
        catch ( Throwable t )
        {
            log.error( "Unable to close Bolt session '" + machine.key() + "'", t );
        }
    }

    private void notifyEnqueued( Job job )
    {
        if ( queueMonitor != null )
        {
            queueMonitor.enqueued( job );
        }
    }

    private void notifyDequeued( Job job )
    {
        if ( queueMonitor != null )
        {
            queueMonitor.dequeued( job );
        }
    }

    private void notifyDrained( List<Job> jobs )
    {
        if ( queueMonitor != null )
        {
            queueMonitor.drained( jobs );
        }
    }

}
