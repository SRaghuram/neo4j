/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source.jvm;

import com.neo4j.metrics.source.MetricGroup;
import com.neo4j.metrics.source.Metrics;

/**
 * These metrics are environment dependent and they may vary on different hardware and with JVM configurations.
 * Typically these metrics will show information about garbage collections
 * (for example the number of events and time spent collecting), memory pools and buffers, and
 * finally the number of active threads running.
 */
public abstract class JvmMetrics extends Metrics
{
    public JvmMetrics()
    {
        super( MetricGroup.JVM );
    }

    static final String VM_NAME_PREFIX = "vm";

    static String prettifyName( String name )
    {
        return name.toLowerCase().replace( ' ', '_' );
    }
}
