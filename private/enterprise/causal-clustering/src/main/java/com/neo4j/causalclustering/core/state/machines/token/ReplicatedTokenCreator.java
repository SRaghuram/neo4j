/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.machines.token;

import org.neo4j.kernel.api.txstate.TransactionState;

public interface ReplicatedTokenCreator
{
    void createToken( TransactionState txState, String tokenName, boolean internal, int tokenId );
}
