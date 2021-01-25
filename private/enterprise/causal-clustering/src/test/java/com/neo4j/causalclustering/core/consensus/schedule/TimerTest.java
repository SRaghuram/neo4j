/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.schedule;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.LifeExtension;
import org.neo4j.util.concurrent.BinaryLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.kernel.impl.scheduler.JobSchedulerFactory.createScheduler;
import static org.neo4j.logging.NullLog.getInstance;

/**
 * Most aspects of the Timer are tested through the {@link TimerServiceTest}.
 */
@ExtendWith( LifeExtension.class )
class TimerTest
{
    @Inject
    private LifeSupport life;

    @Test
    void shouldHandleConcurrentResetAndInvocationOfHandler()
    {
        // given
        JobScheduler scheduler = life.add( createScheduler() );

        BinaryLatch invoked = new BinaryLatch();
        BinaryLatch done = new BinaryLatch();

        TimeoutHandler handler = timer ->
        {
            invoked.release();
            done.await();
        };

        Timer timer = new Timer( () -> "test", scheduler, getInstance(), Group.RAFT_HANDLER, handler );
        timer.set( new FixedTimeout( 0, SECONDS ) );
        invoked.await();

        // when
        timer.reset();

        // then: should not deadlock

        // cleanup
        done.release();
    }
}
