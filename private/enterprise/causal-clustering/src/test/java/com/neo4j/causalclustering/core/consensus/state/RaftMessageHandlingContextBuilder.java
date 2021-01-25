/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.state;

import com.neo4j.configuration.CausalClusteringSettings;

import java.io.IOException;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.configuration.Config;

import static com.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.builder;
import static com.neo4j.configuration.ServerGroupsSupplier.listen;

public class RaftMessageHandlingContextBuilder
{
    public static RaftMessageHandlingContextBuilder contextBuilder()
    {
        return new RaftMessageHandlingContextBuilder( builder() );
    }

    public static RaftMessageHandlingContextBuilder contextBuilder( ReadableRaftState state )
    {
        return new RaftMessageHandlingContextBuilder( state );
    }

    public static RaftMessageHandlingContextBuilder contextBuilder( RaftStateBuilder stateBuilder )
    {
        return new RaftMessageHandlingContextBuilder( stateBuilder );
    }

    public static RaftMessageHandlingContext contextWithState( ReadableRaftState state ) throws IOException
    {
        return new RaftMessageHandlingContextBuilder( state ).build();
    }

    public static RaftMessageHandlingContext contextWithStateWithPreVote( ReadableRaftState state ) throws IOException
    {
        return new RaftMessageHandlingContextBuilder( state ).supportsPreVoting( true ).build();
    }

    private ReadableRaftState state;
    private RaftStateBuilder stateBuilder;
    private boolean supportsPreVoting;
    private BooleanSupplier shutdownInProgressSupplier = () -> false;
    private BooleanSupplier isReadOnly = () -> false;

    private RaftMessageHandlingContextBuilder( ReadableRaftState state )
    {
        this.state = state;
    }

    private RaftMessageHandlingContextBuilder( RaftStateBuilder stateBuilder )
    {
        this.stateBuilder = stateBuilder;
    }

    public RaftMessageHandlingContext build() throws IOException
    {
        var config = Config.newBuilder()
                .set( CausalClusteringSettings.enable_pre_voting, supportsPreVoting )
                .build();
        if ( state == null )
        {
            state = stateBuilder.build();
        }
        return new RaftMessageHandlingContext( state, config, listen( config ), shutdownInProgressSupplier, isReadOnly );
    }

    public RaftMessageHandlingContextBuilder supportsPreVoting( boolean supportsPreVoting )
    {
        this.supportsPreVoting = supportsPreVoting;
        return this;
    }

    public RaftMessageHandlingContextBuilder shutdownInProgress( boolean shutdownInProgress )
    {
        this.shutdownInProgressSupplier = () -> shutdownInProgress;
        return this;
    }

    public RaftMessageHandlingContextBuilder isReadOnly( boolean isReadOnly )
    {
        this.isReadOnly = () -> isReadOnly;
        return this;
    }
}
