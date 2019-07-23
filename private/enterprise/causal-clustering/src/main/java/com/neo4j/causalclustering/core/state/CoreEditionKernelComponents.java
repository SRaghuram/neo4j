/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.state.machines.CoreStateMachines;

import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.token.TokenHolders;

public class CoreEditionKernelComponents
{
    private final CommitProcessFactory commitProcessFactory;
    private final Locks lockManager;
    private final TokenHolders tokenHolders;
    private final DatabaseIdContext idContext;
    private final CoreStateMachines stateMachines;

    public CoreEditionKernelComponents( CommitProcessFactory commitProcessFactory, Locks lockManager, TokenHolders tokenHolders, DatabaseIdContext idContext,
            CoreStateMachines stateMachines )
    {
        this.commitProcessFactory = commitProcessFactory;
        this.lockManager = lockManager;
        this.tokenHolders = tokenHolders;
        this.idContext = idContext;
        this.stateMachines = stateMachines;
    }

    public DatabaseIdContext idContext()
    {
        return idContext;
    }

    public CommitProcessFactory commitProcessFactory()
    {
        return commitProcessFactory;
    }

    public TokenHolders tokenHolders()
    {
        return tokenHolders;
    }

    public Locks lockManager()
    {
        return lockManager;
    }

    public CoreStateMachines stateMachines()
    {
        return stateMachines;
    }
}
