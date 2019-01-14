/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.roles;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.outcome.Outcome;
import org.neo4j.causalclustering.core.consensus.outcome.PruneLogCommand;

class Pruning
{

    private Pruning()
    {
    }

    static void handlePruneRequest( Outcome outcome, RaftMessages.PruneRequest pruneRequest )
    {
        outcome.addLogCommand( new PruneLogCommand( pruneRequest.pruneIndex() ) );
    }
}
