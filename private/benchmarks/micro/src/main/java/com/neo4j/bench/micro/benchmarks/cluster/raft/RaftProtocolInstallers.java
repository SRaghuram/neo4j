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
import com.neo4j.causalclustering.core.consensus.protocol.RaftProtocolClientInstaller;
import com.neo4j.causalclustering.core.consensus.protocol.RaftProtocolServerInstaller;
import com.neo4j.causalclustering.messaging.Inbound;
import com.neo4j.causalclustering.messaging.marshalling.v2.SupportedMessagesV2;
import com.neo4j.causalclustering.messaging.marshalling.DecodingDispatcher;
import com.neo4j.causalclustering.messaging.marshalling.RaftMessageComposer;
import com.neo4j.causalclustering.messaging.marshalling.RaftMessageDecoder;
import com.neo4j.causalclustering.messaging.marshalling.RaftMessageEncoder;
import com.neo4j.causalclustering.messaging.marshalling.v3.SupportedMessagesV3;
import com.neo4j.causalclustering.messaging.marshalling.v3.decoding.RaftMessageDecoderV3;
import com.neo4j.causalclustering.messaging.marshalling.v3.encoding.RaftMessageEncoderV3;
import com.neo4j.causalclustering.messaging.marshalling.v4.decoding.RaftMessageDecoderV4;
import com.neo4j.causalclustering.messaging.marshalling.v4.encoding.RaftMessageEncoderV4;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;

import java.time.Clock;

import org.neo4j.logging.LogProvider;

import static com.neo4j.causalclustering.messaging.marshalling.SupportedMessages.SUPPORT_ALL;
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
            return new RaftProtocolClientInstaller( pipelineBuilderFactory,
                                                    emptyList(),
                                                    logProvider,
                                                    new SupportedMessagesV2(),
                                                    () -> new RaftMessageEncoder() );
        }
        case V3:
        {
            return new RaftProtocolClientInstaller( pipelineBuilderFactory,
                                                    emptyList(),
                                                    logProvider,
                                                    new SupportedMessagesV3(),
                                                    () -> new RaftMessageEncoderV3() );
        }
        case V4:
            return new RaftProtocolClientInstaller( pipelineBuilderFactory,
                                                    emptyList(),
                                                    logProvider,
                                                    SUPPORT_ALL,
                                                    () -> new RaftMessageEncoderV4() );
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
            return new RaftProtocolServerInstaller( raftMessageNettyHandler,
                                                    pipelineBuilderFactory,
                                                    emptyList(),
                                                    logProvider,
                                                    c -> new DecodingDispatcher( c, logProvider, RaftMessageDecoder::new ),
                                                    () -> new RaftMessageComposer( Clock.systemUTC() ) );
        }
        else if ( version == ProtocolVersion.V3 )
        {
            return new RaftProtocolServerInstaller( raftMessageNettyHandler,
                                                    pipelineBuilderFactory,
                                                    emptyList(),
                                                    logProvider,
                                                    c -> new DecodingDispatcher( c, logProvider, RaftMessageDecoderV3::new ),
                                                    () -> new RaftMessageComposer( Clock.systemUTC() ) );
        }
        else if ( version == ProtocolVersion.V4 )
        {
            new RaftProtocolServerInstaller( raftMessageNettyHandler,
                                             pipelineBuilderFactory,
                                             emptyList(),
                                             logProvider,
                                             c -> new DecodingDispatcher( c, logProvider, RaftMessageDecoderV4::new ),
                                             () -> new RaftMessageComposer( Clock.systemUTC() ) );
        }
        throw new IllegalArgumentException( "Can't handle: " + version );
    }
}
