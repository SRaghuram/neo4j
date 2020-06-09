/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.global;

import com.codahale.metrics.MetricRegistry;

public interface MetricsManager
{
    MetricRegistry getRegistry();

    boolean isConfigured();
}
