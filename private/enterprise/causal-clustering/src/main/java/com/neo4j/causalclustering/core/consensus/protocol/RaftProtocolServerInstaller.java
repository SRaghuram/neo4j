/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.protocol;

import com.neo4j.causalclustering.catchup.RequestDecoderDispatcher;
import com.neo4j.causalclustering.messaging.marshalling.ContentType;
import com.neo4j.causalclustering.messaging.marshalling.ContentTypeProtocol;
import com.neo4j.causalclustering.messaging.marshalling.ContentTypeDispatcher;
import com.neo4j.causalclustering.messaging.marshalling.ReplicatedContentDecoder;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftProtocolServerInstaller implements ProtocolInstaller<ProtocolInstaller.Orientation.Server>
{
    public static class Factory extends ProtocolInstaller.Factory<Orientation.Server,RaftProtocolServerInstaller>
    {
        public Factory( ChannelInboundHandler raftMessageHandler,
                        NettyPipelineBuilderFactory pipelineBuilderFactory,
                        LogProvider logProvider,
                        Function<ContentTypeProtocol,RequestDecoderDispatcher<ContentType>> decodingDispatcher,
                        Supplier<MessageToMessageDecoder<Object>> messageComposer,
                        ApplicationProtocol applicationProtocol )
        {
            super( applicationProtocol, modifiers ->
                    new RaftProtocolServerInstaller( raftMessageHandler,
                                                     pipelineBuilderFactory,
                                                     modifiers,
                                                     logProvider,
                                                     decodingDispatcher,
                                                     messageComposer
                    ) );
        }
    }

    private final ChannelInboundHandler raftMessageHandler;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final List<ModifierProtocolInstaller<Orientation.Server>> modifiers;
    private final Function<ContentTypeProtocol,RequestDecoderDispatcher<ContentType>> decodingDispatcher;
    private final Supplier<MessageToMessageDecoder<Object>> messageComposer;
    private final Log log;

    public RaftProtocolServerInstaller( ChannelInboundHandler raftMessageHandler,
                                        NettyPipelineBuilderFactory pipelineBuilderFactory,
                                        List<ModifierProtocolInstaller<Orientation.Server>> modifiers,
                                        LogProvider logProvider,
                                        Function<ContentTypeProtocol,RequestDecoderDispatcher<ContentType>> decodingDispatcher,
                                        Supplier<MessageToMessageDecoder<Object>> messageComposer )
    {
        this.raftMessageHandler = raftMessageHandler;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.modifiers = modifiers;
        this.decodingDispatcher = decodingDispatcher;
        this.messageComposer = messageComposer;
        this.log = logProvider.getLog( getClass() );
    }

    /**
     * Uses latest version of handlers. Hence version naming may be less than the current version if no change was needed for that handler
     */
    @Override
    public void install( Channel channel )
    {

        ContentTypeProtocol contentTypeProtocol = new ContentTypeProtocol();

        pipelineBuilderFactory
                .server( channel, log )
                .modify( modifiers )
                .addFraming()
                .add( "raft_content_type_dispatcher", new ContentTypeDispatcher( contentTypeProtocol ) )
                .add( "raft_component_decoder", decodingDispatcher.apply( contentTypeProtocol ) )
                .add( "raft_content_decoder", new ReplicatedContentDecoder( contentTypeProtocol ) )
                .add( "raft_message_composer", messageComposer.get() )
                .add( "raft_handler", raftMessageHandler )
                .install();
    }

    @Override
    public Collection<Collection<ModifierProtocol>> modifiers()
    {
        return modifiers.stream().map( ModifierProtocolInstaller::protocols ).collect( Collectors.toList() );
    }
}
