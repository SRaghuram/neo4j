/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import java.time.Duration;

public interface RaftMessageProcessingMonitor
{
    void setDelay( Duration delay );

    void updateTimer( RaftMessages.Type type, Duration duration );
}
