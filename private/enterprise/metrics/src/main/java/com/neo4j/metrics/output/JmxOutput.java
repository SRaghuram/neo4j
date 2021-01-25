/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.output;

import com.codahale.metrics.jmx.JmxReporter;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;

class JmxOutput extends LifecycleAdapter
{
    private final JmxReporter jmxReporter;

    JmxOutput( JmxReporter jmxReporter )
    {
        this.jmxReporter = jmxReporter;
    }

    @Override
    public void start()
    {
        jmxReporter.start();
    }

    @Override
    public void stop()
    {
        jmxReporter.stop();
    }
}
