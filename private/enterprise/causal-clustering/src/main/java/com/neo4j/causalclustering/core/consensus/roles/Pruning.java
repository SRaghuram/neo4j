/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.roles;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.outcome.OutcomeBuilder;
import com.neo4j.causalclustering.core.consensus.outcome.PruneLogCommand;

class Pruning
{

    private Pruning()
    {
    }

    static void handlePruneRequest( OutcomeBuilder outcomeBuilder, RaftMessages.PruneRequest pruneRequest )
    {
        outcomeBuilder.addLogCommand( new PruneLogCommand( pruneRequest.pruneIndex() ) );
    }
}
