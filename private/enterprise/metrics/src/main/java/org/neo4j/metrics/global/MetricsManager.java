/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.metrics.global;

import com.codahale.metrics.MetricRegistry;

import org.neo4j.metrics.output.EventReporter;

public interface MetricsManager
{
    EventReporter getReporter();

    MetricRegistry getRegistry();

    boolean isConfigured();
}
