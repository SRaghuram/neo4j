/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.state;

import com.neo4j.configuration.CausalClusteringSettings;
import com.neo4j.configuration.ServerGroupName;
import com.neo4j.configuration.ServerGroupsSupplier;

import java.util.Set;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;

import static java.util.Set.copyOf;

public class RaftMessageHandlingContext
{
    private final ReadableRaftState state;
    private final boolean supportPreVoting;
    private final boolean refuseToBeLeader;
    private final ServerGroupsSupplier serverGroupsSupplier;
    private final Supplier<Boolean> shutdownInProgressSupplier;

    public RaftMessageHandlingContext( ReadableRaftState state, Config config,
            ServerGroupsSupplier serverGroupsSupplier, Supplier<Boolean> shutdownInProgressSupplier )
    {
        this.state = state;
        this.supportPreVoting = config.get( CausalClusteringSettings.enable_pre_voting );
        this.refuseToBeLeader = config.get( CausalClusteringSettings.refuse_to_be_leader );
        this.serverGroupsSupplier = serverGroupsSupplier;
        this.shutdownInProgressSupplier = shutdownInProgressSupplier;
    }

    public ReadableRaftState state()
    {
        return state;
    }

    public boolean supportPreVoting()
    {
        return supportPreVoting;
    }

    public boolean refusesToBeLeader()
    {
        return refuseToBeLeader;
    }

    public Set<ServerGroupName> serverGroups()
    {
        return serverGroupsSupplier.get();
    }

    public boolean isProcessShutdownInProgress()
    {
        return shutdownInProgressSupplier.get();
    }
}
