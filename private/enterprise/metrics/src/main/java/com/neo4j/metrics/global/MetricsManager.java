/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.metrics.global;

import com.neo4j.metrics.metric.MetricsRegister;

public interface MetricsManager
{
    MetricsRegister getRegistry();

    boolean isConfigured();
}
