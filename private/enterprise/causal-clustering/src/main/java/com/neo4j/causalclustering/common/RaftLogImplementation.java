/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.core.consensus.log.InMemoryRaftLog;
import com.neo4j.causalclustering.core.consensus.log.segmented.SegmentedRaftLog;

/**
 * Enum for selecting the way in which this cluster members Raft log is stored.
 * See {@link InMemoryRaftLog} and {@link SegmentedRaftLog}
 */
public enum RaftLogImplementation
{
    IN_MEMORY, SEGMENTED
}
