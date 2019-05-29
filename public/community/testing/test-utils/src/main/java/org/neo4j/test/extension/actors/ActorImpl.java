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
package org.neo4j.test.extension.actors;

import java.lang.reflect.Executable;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

class ActorImpl implements Actor
{
    private static final FutureTask<Void> STOP_SIGNAL = new FutureTask<>( () -> null );
    private final LinkedTransferQueue<FutureTask<?>> queue;
    private final Thread thread;
    private final AtomicBoolean started = new AtomicBoolean();
    private volatile boolean stopped;
    private volatile boolean executing;

    ActorImpl( ThreadGroup threadGroup, String name )
    {
        queue = new LinkedTransferQueue<>();
        thread = new Thread( threadGroup, this::runActor, name );
    }

    private <T> void enqueue( FutureTask<T> task )
    {
        queue.offer( task );
        if ( !started.get() && started.compareAndSet( false, true ) )
        {
            thread.start();
        }
    }

    private void runActor()
    {
        try
        {
            FutureTask<?> task;
            while ( !stopped && (task = queue.take()) != STOP_SIGNAL )
            {
                try
                {
                    executing = true;
                    task.run();
                }
                finally
                {
                    executing = false;
                }
            }
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    public void stop()
    {
        stopped = true;
        enqueue( STOP_SIGNAL );
    }

    public void join() throws InterruptedException
    {
        thread.join();
    }

    @Override
    public <T> Future<T> submit( Callable<T> callable )
    {
        FutureTask<T> task = new FutureTask<>( callable );
        enqueue( task );
        return task;
    }

    @Override
    public <T> Future<T> submit( Runnable runnable, T result )
    {
        FutureTask<T> task = new FutureTask<>( runnable, result );
        enqueue( task );
        return task;
    }

    @Override
    public Future<Void> submit( Runnable runnable )
    {
        return submit( runnable, null );
    }

    @Override
    public void untilWaiting() throws InterruptedException
    {
        do
        {
            Thread.State state = thread.getState();
            if ( executing && (state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING) )
            {
                return;
            }
            if ( state == Thread.State.TERMINATED )
            {
                throw new AssertionError( "Actor thread " + thread.getName() + " has terminated." );
            }
            if ( state == Thread.State.NEW )
            {
                throw new IllegalStateException( "Actor thread " + thread.getName() + " has not yet started." );
            }
            if ( Thread.interrupted() )
            {
                throw new InterruptedException();
            }
            Thread.onSpinWait();
        }
        while ( true );
    }

    @Override
    public void untilWaitingIn( Executable constructorOrMethod ) throws InterruptedException
    {
        do
        {
            untilWaiting();
            if ( isIn( constructorOrMethod ) )
            {
                return;
            }
            Thread.sleep( 1 );
        }
        while ( true );
    }

    private boolean isIn( Executable constructorOrMethod )
    {
        String targetMethodName = constructorOrMethod.getName();
        String targetClassName = constructorOrMethod.getDeclaringClass().getName();
        StackTraceElement[] stackTrace = thread.getStackTrace();
        for ( StackTraceElement element : stackTrace )
        {
            if ( element.getMethodName().equals( targetMethodName ) && element.getClassName().equals( targetClassName ) )
            {
                return true;
            }
        }
        return false;
    }
}
