/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.raft;

import com.neo4j.bench.micro.benchmarks.cluster.ProtocolInstallers;
import com.neo4j.bench.micro.benchmarks.cluster.ProtocolVersion;
import com.neo4j.causalclustering.core.consensus.RaftMessageNettyHandler;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolClientInstallerV2;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolServerInstallerV2;
import com.neo4j.causalclustering.core.consensus.protocol.v3.RaftProtocolClientInstallerV3;
import com.neo4j.causalclustering.core.consensus.protocol.v4.RaftProtocolClientInstallerV4;
import com.neo4j.causalclustering.messaging.Inbound;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;

import org.neo4j.logging.LogProvider;

import static java.util.Collections.emptyList;

public class RaftProtocolInstallers implements ProtocolInstallers
{
    private final ProtocolVersion version;
    private final Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> handler;
    private final LogProvider logProvider;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;

    RaftProtocolInstallers( ProtocolVersion version,
                            Inbound.MessageHandler<RaftMessages.InboundRaftMessageContainer<?>> handler,
                            LogProvider logProvider )
    {
        this.version = version;
        this.handler = handler;
        this.logProvider = logProvider;
        this.pipelineBuilderFactory = NettyPipelineBuilderFactory.insecure();
    }

    @Override
    public ProtocolInstaller<ProtocolInstaller.Orientation.Client> clientInstaller()
    {
        switch ( version )
        {
        case V2:
        {
            return new RaftProtocolClientInstallerV2( pipelineBuilderFactory, emptyList(), logProvider );
        }
        case V3:
        {
            return new RaftProtocolClientInstallerV3( pipelineBuilderFactory, emptyList(), logProvider );
        }
        case V4:
            return new RaftProtocolClientInstallerV4( pipelineBuilderFactory, emptyList(), logProvider );
        default:
        {
            throw new IllegalArgumentException( "Can't handle: " + version );
        }
        }
    }

    @Override
    public ProtocolInstaller<ProtocolInstaller.Orientation.Server> serverInstaller()
    {
        RaftMessageNettyHandler raftMessageNettyHandler = new RaftMessageNettyHandler( logProvider );
        raftMessageNettyHandler.registerHandler( handler );

        if ( version == ProtocolVersion.V2 )
        {
            return new RaftProtocolServerInstallerV2( raftMessageNettyHandler, pipelineBuilderFactory, emptyList(), logProvider );
        }
        throw new IllegalArgumentException( "Can't handle: " + version );
    }
}
