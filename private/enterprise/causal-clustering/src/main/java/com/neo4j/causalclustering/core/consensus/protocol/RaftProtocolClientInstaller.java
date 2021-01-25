/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.protocol;

import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.messaging.marshalling.ReplicatedContentCodec;
import com.neo4j.causalclustering.messaging.marshalling.ContentTypeEncoder;
import com.neo4j.causalclustering.messaging.marshalling.RaftMessageContentEncoder;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;
import io.netty.channel.Channel;
import io.netty.handler.codec.MessageToByteEncoder;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftProtocolClientInstaller implements ProtocolInstaller<ProtocolInstaller.Orientation.Client>
{
    public static class Factory extends ProtocolInstaller.Factory<Orientation.Client,RaftProtocolClientInstaller>
    {

        public Factory( NettyPipelineBuilderFactory clientPipelineBuilderFactory,
                        LogProvider logProvider,
                        RaftMessages.Handler<Boolean,Exception> supportedMessages,
                        Supplier<MessageToByteEncoder<RaftMessages.OutboundRaftMessageContainer>> messageEncoder,
                        ApplicationProtocols applicationProtocol )
        {
            super( applicationProtocol, mods ->
                    new RaftProtocolClientInstaller( clientPipelineBuilderFactory,
                                                     mods,
                                                     logProvider,
                                                     supportedMessages,
                                                     messageEncoder )
            );
        }
    }

    private final List<ModifierProtocolInstaller<Orientation.Client>> modifiers;
    private final RaftMessages.Handler<Boolean,Exception> supportedMessages;
    private final NettyPipelineBuilderFactory clientPipelineBuilderFactory;
    private final Supplier<MessageToByteEncoder<RaftMessages.OutboundRaftMessageContainer>> messageEncoder;
    private final Log log;

    public RaftProtocolClientInstaller( NettyPipelineBuilderFactory clientPipelineBuilderFactory,
                                        List<ModifierProtocolInstaller<Orientation.Client>> modifiers,
                                        LogProvider logProvider,
                                        RaftMessages.Handler<Boolean,Exception> supportedMessages,
                                        Supplier<MessageToByteEncoder<RaftMessages.OutboundRaftMessageContainer>> messageEncoder )
    {
        this.modifiers = modifiers;
        this.log = logProvider.getLog( getClass() );
        this.clientPipelineBuilderFactory = clientPipelineBuilderFactory;
        this.supportedMessages = supportedMessages;
        this.messageEncoder = messageEncoder;
    }

    /**
     * Uses latest version of handlers. Hence version naming may be less than the current version if no change was needed for that handler
     */
    @Override
    public void install( Channel channel )
    {
        clientPipelineBuilderFactory
                .client( channel, log )
                .modify( modifiers )
                .addFraming()
                .add( "raft_message_encoder", messageEncoder.get() )
                .add( "raft_content_type_encoder", new ContentTypeEncoder() )
                .add( "raft_chunked_writer", new ChunkedWriteHandler() )
                .add( "raft_message_content_encoder", new RaftMessageContentEncoder( new ReplicatedContentCodec() ) )
                .add( "message_validator", new SupportedMessageHandler( supportedMessages ) )
                .install();
    }

    @Override
    public Collection<Collection<ModifierProtocol>> modifiers()
    {
        return modifiers.stream().map( ModifierProtocolInstaller::protocols ).collect( Collectors.toList() );
    }
}
