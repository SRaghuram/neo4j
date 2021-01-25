/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.tx;

import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import com.neo4j.causalclustering.core.state.CommandDispatcher;
import com.neo4j.causalclustering.core.state.StateMachineResult;

import java.util.function.Consumer;

import org.neo4j.kernel.database.DatabaseId;

public interface CoreReplicatedContent extends ReplicatedContent
{
    void dispatch( CommandDispatcher commandDispatcher, long commandIndex, Consumer<StateMachineResult> callback );

    //TODO: Should this instead exist on Replicated Content, and if so how do we handle e.g. MemberIdSet?
    DatabaseId databaseId();
}
