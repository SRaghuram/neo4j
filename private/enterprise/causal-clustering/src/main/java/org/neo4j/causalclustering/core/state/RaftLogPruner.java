/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state;

import java.io.IOException;
import java.time.Clock;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.RaftMessages;

public class RaftLogPruner
{
    private final RaftMachine raftMachine;
    private final CommandApplicationProcess applicationProcess;
    private final Clock clock;

    public RaftLogPruner( RaftMachine raftMachine, CommandApplicationProcess applicationProcess, Clock clock )
    {

        this.raftMachine = raftMachine;
        this.applicationProcess = applicationProcess;
        this.clock = clock;
    }

    public void prune() throws IOException
    {
        raftMachine.handle( RaftMessages.ReceivedInstantAwareMessage.of(
                clock.instant(),
                new RaftMessages.PruneRequest( applicationProcess.lastFlushed() )
        ) );
    }
}
