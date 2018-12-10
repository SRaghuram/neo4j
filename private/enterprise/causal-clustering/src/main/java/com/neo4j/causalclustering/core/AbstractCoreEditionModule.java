/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.ClusteringEditionModule;
import com.neo4j.causalclustering.core.consensus.ConsensusModule;
import com.neo4j.causalclustering.core.state.CoreStateService;

import org.neo4j.util.VisibleForTesting;

public abstract class AbstractCoreEditionModule extends ClusteringEditionModule
{
    abstract ConsensusModule consensusModule();

    abstract CoreStateService coreStateComponents();

    @VisibleForTesting
    abstract void disableCatchupServer() throws Throwable;
}
