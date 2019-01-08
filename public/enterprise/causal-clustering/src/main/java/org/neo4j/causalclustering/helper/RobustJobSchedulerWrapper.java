/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.helper;

import java.util.concurrent.CancellationException;

import org.neo4j.function.ThrowingAction;
import org.neo4j.kernel.impl.util.JobScheduler;
import org.neo4j.logging.Log;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.POOLED;

/**
 * A robust job catches and logs any exceptions, but keeps running if the job
 * is a recurring one. Any exceptions also end up in the supplied log instead
 * of falling through to syserr. Remaining Throwables (generally errors) are
 * logged but fall through, so that recurring jobs are stopped and the error
 * gets double visibility.
 */
public class RobustJobSchedulerWrapper
{
    private final JobScheduler delegate;
    private final Log log;

    public RobustJobSchedulerWrapper( JobScheduler delegate, Log log )
    {
        this.delegate = delegate;
        this.log = log;
    }

    public JobScheduler.JobHandle schedule( String name, long delayMillis, ThrowingAction<Exception> action )
    {
        return delegate.schedule( new JobScheduler.Group( name, POOLED ),
                () -> withErrorHandling( action ), delayMillis, MILLISECONDS );
    }

    public JobScheduler.JobHandle scheduleRecurring( String name, long periodMillis, ThrowingAction<Exception> action )
    {
        return delegate.scheduleRecurring( new JobScheduler.Group( name, POOLED ),
                () -> withErrorHandling( action ), periodMillis, MILLISECONDS );
    }

    /**
     * Last line of defense error handling.
     */
    private void withErrorHandling( ThrowingAction<Exception> action )
    {
        try
        {
            action.apply();
        }
        catch ( Exception e )
        {
            log.warn( "Uncaught exception", e );
        }
        catch ( Throwable t )
        {
            log.error( "Uncaught error rethrown", t );
            throw t;
        }
    }
}
