/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.protocol.v2;

import com.neo4j.causalclustering.messaging.marshalling.v2.ContentTypeProtocol;
import com.neo4j.causalclustering.messaging.marshalling.v2.decoding.ContentTypeDispatcher;
import com.neo4j.causalclustering.messaging.marshalling.v2.decoding.DecodingDispatcher;
import com.neo4j.causalclustering.messaging.marshalling.v2.decoding.RaftMessageComposer;
import com.neo4j.causalclustering.messaging.marshalling.v2.decoding.ReplicatedContentDecoder;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;

import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftProtocolServerInstallerV2 implements ProtocolInstaller<ProtocolInstaller.Orientation.Server>
{
    private static final ApplicationProtocols APPLICATION_PROTOCOL = ApplicationProtocols.RAFT_2_0;
    private final LogProvider logProvider;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Server,RaftProtocolServerInstallerV2>
    {
        public Factory( ChannelInboundHandler raftMessageHandler, NettyPipelineBuilderFactory pipelineBuilderFactory, LogProvider logProvider )
        {
            super( APPLICATION_PROTOCOL, modifiers -> new RaftProtocolServerInstallerV2( raftMessageHandler, pipelineBuilderFactory, modifiers, logProvider ) );
        }
    }

    private final ChannelInboundHandler raftMessageHandler;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final List<ModifierProtocolInstaller<Orientation.Server>> modifiers;
    private final Log log;

    public RaftProtocolServerInstallerV2( ChannelInboundHandler raftMessageHandler, NettyPipelineBuilderFactory pipelineBuilderFactory,
            List<ModifierProtocolInstaller<Orientation.Server>> modifiers, LogProvider logProvider )
    {
        this.raftMessageHandler = raftMessageHandler;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.modifiers = modifiers;
        this.logProvider = logProvider;
        this.log = this.logProvider.getLog( getClass() );
    }

    /**
     * Uses latest version of handlers. Hence version naming may be less than the current version if no change was needed for that handler
     */
    @Override
    public void install( Channel channel ) throws Exception
    {

        ContentTypeProtocol contentTypeProtocol = new ContentTypeProtocol();
        DecodingDispatcher decodingDispatcher = new DecodingDispatcher( contentTypeProtocol, logProvider );
        pipelineBuilderFactory
                .server( channel, log )
                .modify( modifiers )
                .addFraming()
                .add( "raft_content_type_dispatcher", new ContentTypeDispatcher( contentTypeProtocol ) )
                .add( "raft_component_decoder", decodingDispatcher )
                .add( "raft_content_decoder", new ReplicatedContentDecoder( contentTypeProtocol ) )
                .add( "raft_message_composer", new RaftMessageComposer( Clock.systemUTC() ) )
                .add( "raft_handler", raftMessageHandler )
                .install();
    }

    @Override
    public ApplicationProtocol applicationProtocol()
    {
        return APPLICATION_PROTOCOL;
    }

    @Override
    public Collection<Collection<ModifierProtocol>> modifiers()
    {
        return modifiers.stream().map( ModifierProtocolInstaller::protocols ).collect( Collectors.toList() );
    }
}
