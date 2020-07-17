/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.state;

import com.neo4j.configuration.CausalClusteringSettings;

import java.io.IOException;

import org.neo4j.configuration.Config;

import static com.neo4j.causalclustering.core.consensus.state.RaftStateBuilder.builder;

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
    private boolean refuseToBeLeader;
    private boolean supportsPreVoting;

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
                .set( CausalClusteringSettings.refuse_to_be_leader, refuseToBeLeader )
                .build();
        if ( state == null )
        {
            state = stateBuilder.build();
        }
        return new RaftMessageHandlingContext( state, config );
    }

    public RaftMessageHandlingContextBuilder refusesToBeLeader( boolean refuseToBeLeader )
    {
        this.refuseToBeLeader = refuseToBeLeader;
        return this;
    }

    public RaftMessageHandlingContextBuilder supportsPreVoting( boolean supportsPreVoting )
    {
        this.supportsPreVoting = supportsPreVoting;
        return this;
    }
}
