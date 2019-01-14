/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.log.monitoring;

public interface RaftLogAppendIndexMonitor
{
    long appendIndex();

    void appendIndex( long appendIndex );
}

