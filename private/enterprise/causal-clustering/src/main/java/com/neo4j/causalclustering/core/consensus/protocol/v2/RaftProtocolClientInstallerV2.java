/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.protocol.v2;

import com.neo4j.causalclustering.core.consensus.protocol.SupportedMessageHandler;
import com.neo4j.causalclustering.messaging.marshalling.ReplicatedContentCodec;
import com.neo4j.causalclustering.messaging.marshalling.v2.SupportedMessagesV2;
import com.neo4j.causalclustering.messaging.marshalling.v2.encoding.ContentTypeEncoder;
import com.neo4j.causalclustering.messaging.marshalling.v2.encoding.RaftMessageContentEncoder;
import com.neo4j.causalclustering.messaging.marshalling.v2.encoding.RaftMessageEncoder;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocol;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;
import io.netty.channel.Channel;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftProtocolClientInstallerV2 implements ProtocolInstaller<ProtocolInstaller.Orientation.Client>
{
    private static final ApplicationProtocols APPLICATION_PROTOCOL = ApplicationProtocols.RAFT_2_0;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Client,RaftProtocolClientInstallerV2>
    {
        public Factory( NettyPipelineBuilderFactory clientPipelineBuilderFactory, LogProvider logProvider )
        {
            super( APPLICATION_PROTOCOL, mods -> new RaftProtocolClientInstallerV2( clientPipelineBuilderFactory, mods, logProvider ) );
        }
    }

    private final List<ModifierProtocolInstaller<Orientation.Client>> modifiers;
    private final Log log;
    private final NettyPipelineBuilderFactory clientPipelineBuilderFactory;

    public RaftProtocolClientInstallerV2( NettyPipelineBuilderFactory clientPipelineBuilderFactory,
            List<ModifierProtocolInstaller<Orientation.Client>> modifiers, LogProvider logProvider )
    {
        this.modifiers = modifiers;
        this.log = logProvider.getLog( getClass() );
        this.clientPipelineBuilderFactory = clientPipelineBuilderFactory;
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
                .add( "raft_message_encoder", new RaftMessageEncoder() )
                .add( "raft_content_type_encoder", new ContentTypeEncoder() )
                .add( "raft_chunked_writer", new ChunkedWriteHandler() )
                .add( "raft_message_content_encoder", new RaftMessageContentEncoder( new ReplicatedContentCodec() ) )
                .add( "message_validator", new SupportedMessageHandler( new SupportedMessagesV2() ) )
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
