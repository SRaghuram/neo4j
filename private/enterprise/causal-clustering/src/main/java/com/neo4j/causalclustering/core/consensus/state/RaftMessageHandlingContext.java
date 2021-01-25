/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.state;

import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.ServerGroupName;
import com.neo4j.configuration.ServerGroupsSupplier;

import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.ReadOnlyDatabaseChecker;

import static java.util.Set.copyOf;

public class RaftMessageHandlingContext
{
    private final ReadableRaftState state;
    private final boolean supportPreVoting;
    private final ServerGroupsSupplier serverGroupsSupplier;
    private final BooleanSupplier shutdownInProgressSupplier;
    private final BooleanSupplier isReadOnly;

    public RaftMessageHandlingContext( ReadableRaftState state, Config config, ServerGroupsSupplier serverGroupsSupplier,
            BooleanSupplier shutdownInProgressSupplier, BooleanSupplier isReadOnly )
    {
        this.state = state;
        this.supportPreVoting = config.get( CausalClusteringSettings.enable_pre_voting );
        this.serverGroupsSupplier = serverGroupsSupplier;
        this.shutdownInProgressSupplier = shutdownInProgressSupplier;
        this.isReadOnly = isReadOnly;
    }

    public ReadableRaftState state()
    {
        return state;
    }

    public boolean supportPreVoting()
    {
        return supportPreVoting;
    }

    public Set<ServerGroupName> serverGroups()
    {
        return serverGroupsSupplier.get();
    }

    public boolean isProcessShutdownInProgress()
    {
        return shutdownInProgressSupplier.getAsBoolean();
    }

    public boolean isReadOnly()
    {
        return isReadOnly.getAsBoolean();
    }
}
