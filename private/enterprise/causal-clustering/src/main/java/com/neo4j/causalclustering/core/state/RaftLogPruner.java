/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.consensus.RaftMessages;

import java.io.IOException;

public class RaftLogPruner
{
    private final RaftMachine raftMachine;
    private final CommandApplicationProcess applicationProcess;

    public RaftLogPruner( RaftMachine raftMachine, CommandApplicationProcess applicationProcess )
    {

        this.raftMachine = raftMachine;
        this.applicationProcess = applicationProcess;
    }

    public void prune() throws IOException
    {
        raftMachine.handle( new RaftMessages.PruneRequest( applicationProcess.flush() ) );
    }
}
