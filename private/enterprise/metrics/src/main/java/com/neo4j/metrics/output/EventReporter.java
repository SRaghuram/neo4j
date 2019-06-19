/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;

import java.util.SortedMap;

public interface EventReporter
{
    /**
     * To be called whenever an event occurs. Note that this call might be blocking, hence don't call it from
     * time sensitive code or hot path code.
     *
     * @param gauges all of the gauges in the registry
     * @param counters all of the counters in the registry
     * @param histograms all of the histograms in the registry
     * @param meters all of the meters in the registry
     * @param timers all of the timers in the registry
     */
    void report( SortedMap<String,Gauge> gauges,
            SortedMap<String,Counter> counters,
            SortedMap<String,Histogram> histograms,
            SortedMap<String,Meter> meters,
            SortedMap<String,Timer> timers );
}
