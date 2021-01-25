/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.schedule;

import java.util.Collection;

import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.time.FakeClock;

public class OnDemandTimerService extends TimerService
{
    private final FakeClock clock;
    private OnDemandJobScheduler onDemandJobScheduler;

    public OnDemandTimerService( FakeClock clock )
    {
        super( new OnDemandJobScheduler(), NullLogProvider.getInstance() );
        this.clock = clock;
        onDemandJobScheduler = (OnDemandJobScheduler) scheduler;
    }

    @Override
    public void invoke( TimerName name )
    {
        Collection<Timer> timers = getTimers( name );

        for ( Timer timer : timers )
        {
            Delay delay = timer.delay();
            clock.forward( delay.amount(), delay.unit() );
        }

        for ( Timer timer : timers )
        {
            timer.invoke();
        }

        onDemandJobScheduler.runJob();
    }
}
