/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus;

import com.neo4j.causalclustering.core.consensus.outcome.Outcome;
import com.neo4j.causalclustering.core.consensus.state.ReadableRaftState;

import java.io.IOException;

import org.neo4j.logging.Log;

public interface RaftMessageHandler
{
    Outcome handle( RaftMessages.RaftMessage message, ReadableRaftState context, Log log ) throws IOException;
}
