/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.raft;

import com.neo4j.bench.micro.benchmarks.cluster.ProtocolInstallers;
import com.neo4j.bench.micro.benchmarks.cluster.ProtocolVersion;
import com.neo4j.causalclustering.core.consensus.RaftMessageNettyHandler;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.protocol.v1.RaftProtocolClientInstallerV1;
import com.neo4j.causalclustering.core.consensus.protocol.v1.RaftProtocolServerInstallerV1;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolClientInstallerV2;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolServerInstallerV2;
import com.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import com.neo4j.causalclustering.messaging.Inbound;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;

import java.util.Collections;

import org.neo4j.logging.LogProvider;

public class RaftProtocolInstallers implements ProtocolInstallers
{
    private final ProtocolVersion version;
    private final Inbound.MessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> handler;
    private final LogProvider logProvider;

    RaftProtocolInstallers( ProtocolVersion version,
                            Inbound.MessageHandler<RaftMessages.ReceivedInstantClusterIdAwareMessage<?>> handler,
                            LogProvider logProvider )
    {
        this.version = version;
        this.handler = handler;
        this.logProvider = logProvider;
    }

    public ProtocolInstaller<ProtocolInstaller.Orientation.Client> clientInstaller()
    {
        if ( version == ProtocolVersion.V1 )
        {
            return new RaftProtocolClientInstallerV1( new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER ),
                                                      Collections.emptyList(),
                                                      logProvider );
        }
        if ( version == ProtocolVersion.V2 )
        {
            return new RaftProtocolClientInstallerV2( new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER ),
                                                      Collections.emptyList(),
                                                      logProvider );
        }
        throw new IllegalArgumentException( "Can't handle: " + version );
    }

    @Override
    public ProtocolInstaller<ProtocolInstaller.Orientation.Server> serverInstaller()
    {
        RaftMessageNettyHandler raftMessageNettyHandler = new RaftMessageNettyHandler( logProvider );
        raftMessageNettyHandler.registerHandler( handler );

        if ( version == ProtocolVersion.V1 )
        {
            return new RaftProtocolServerInstallerV1( raftMessageNettyHandler,
                                                      new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER ),
                                                      Collections.emptyList(),
                                                      "db-name",
                                                      logProvider );
        }
        if ( version == ProtocolVersion.V2 )
        {
            return new RaftProtocolServerInstallerV2( raftMessageNettyHandler,
                                                      new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER ),
                                                      Collections.emptyList(),
                                                      logProvider );
        }
        throw new IllegalArgumentException( "Can't handle: " + version );
    }
}
