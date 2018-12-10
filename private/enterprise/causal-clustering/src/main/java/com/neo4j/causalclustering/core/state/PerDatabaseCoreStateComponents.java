/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state;

import com.neo4j.causalclustering.core.state.machines.CoreStateMachines;
import com.neo4j.causalclustering.core.state.machines.id.ReplicatedIdRangeAcquirer;

import org.neo4j.graphdb.factory.module.id.DatabaseIdContext;
import org.neo4j.kernel.impl.api.CommitProcessFactory;
import org.neo4j.kernel.impl.core.TokenHolders;
import org.neo4j.kernel.impl.locking.Locks;

public class PerDatabaseCoreStateComponents
{
    private final CommitProcessFactory commitProcessFactory;
    private final CoreStateMachines stateMachines;
    private final Locks lockManager;
    private final TokenHolders tokenHolders;
    private final ReplicatedIdRangeAcquirer rangeAcquirer;
    private final DatabaseIdContext idContext;

    public PerDatabaseCoreStateComponents( CommitProcessFactory commitProcessFactory, CoreStateMachines stateMachines, TokenHolders tokenHolders,
            ReplicatedIdRangeAcquirer rangeAcquirer, Locks lockManager, DatabaseIdContext idContext )
    {
        this.commitProcessFactory = commitProcessFactory;
        this.stateMachines = stateMachines;
        this.lockManager = lockManager;
        this.tokenHolders = tokenHolders;
        this.rangeAcquirer = rangeAcquirer;
        this.idContext = idContext;
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

    public CoreStateMachines stateMachines()
    {
        return stateMachines;
    }

    public Locks lockManager()
    {
        return lockManager;
    }

    public ReplicatedIdRangeAcquirer rangeAcquirer()
    {
        return rangeAcquirer;
    }
}
