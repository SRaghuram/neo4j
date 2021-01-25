/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.schedule;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

public class CountingTimerService extends TimerService
{
    private final Map<String,Long> counts = new HashMap<>();

    public CountingTimerService( JobScheduler scheduler, LogProvider logProvider )
    {
        super( scheduler, logProvider );
    }

    @Override
    public Timer create( TimerName name, Group group, TimeoutHandler handler )
    {
        TimeoutHandler countingHandler = timer -> {
            long count = counts.getOrDefault( name.name(), 0L );
            counts.put( name.name(), count + 1 );
            handler.onTimeout( timer );
        };
        return super.create( name, group, countingHandler );
    }

    public long invocationCount( TimerName name )
    {
        return counts.getOrDefault( name.name(), 0L );
    }
}
