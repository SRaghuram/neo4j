/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.state.machines.tx;

import java.util.function.Consumer;

import org.neo4j.causalclustering.core.replication.ReplicatedContent;
import org.neo4j.causalclustering.core.state.CommandDispatcher;
import org.neo4j.causalclustering.core.state.Result;

public interface CoreReplicatedContent extends ReplicatedContent
{
    void dispatch( CommandDispatcher commandDispatcher, long commandIndex, Consumer<Result> callback );

    //TODO: Should this instead exist on Replicated Content, and if so how do we handle e.g. MemberIdSet?
    String databaseName();
}
