/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.source;

import org.neo4j.annotations.service.Service;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * Anyone extending this class while adding new metrics should annotate the new class
 * with ServiceProvider, provide a zero-argument constructor and use Documented annotation
 * on both the class and the Strings representing the metric names.
 */
@Service
public abstract class Metrics extends LifecycleAdapter
{
    private final MetricGroup group;

    public Metrics( MetricGroup group )
    {
        this.group = group;
    }

    public MetricGroup getGroup()
    {
        return group;
    }

    /**
     * Only used for generating documentation.
     * When generating the documentation for metrics, the Documented static Strings
     * are taken as the metric names directly.
     * Override this in classes that wants to modify the metric name shown.
     */
    public String modifyDocumentedMetricName( String metric )
    {
        return metric;
    }

    @Override
    public abstract void start();

    @Override
    public abstract void stop();
}
