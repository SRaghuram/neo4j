/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.protocol.v2;

import io.netty.channel.Channel;
import io.netty.handler.stream.ChunkedWriteHandler;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.messaging.marshalling.v2.encoding.ContentTypeEncoder;
import org.neo4j.causalclustering.messaging.marshalling.v2.encoding.RaftMessageContentEncoder;
import org.neo4j.causalclustering.messaging.marshalling.v2.encoding.RaftMessageEncoder;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.neo4j.causalclustering.messaging.marshalling.CoreReplicatedContentMarshalFactory.codecV2;

public class RaftProtocolClientInstallerV2 implements ProtocolInstaller<Orientation.Client>
{
    private static final Protocol.ApplicationProtocols APPLICATION_PROTOCOL = Protocol.ApplicationProtocols.RAFT_2;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Client,RaftProtocolClientInstallerV2>
    {
        public Factory( NettyPipelineBuilderFactory clientPipelineBuilderFactory, LogProvider logProvider )
        {
            super( APPLICATION_PROTOCOL, modifiers -> new RaftProtocolClientInstallerV2( clientPipelineBuilderFactory, modifiers, logProvider ) );
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

    @Override
    public void install( Channel channel ) throws Exception
    {
        clientPipelineBuilderFactory
                .client( channel, log )
                .modify( modifiers )
                .addFraming()
                .add( "raft_message_encoder", new RaftMessageEncoder() )
                .add( "raft_content_type_encoder", new ContentTypeEncoder() )
                .add( "raft_chunked_writer", new ChunkedWriteHandler(  ) )
                .add( "raft_message_content_encoder", new RaftMessageContentEncoder( codecV2() ) )
                .install();
    }

    @Override
    public Protocol.ApplicationProtocol applicationProtocol()
    {
        return APPLICATION_PROTOCOL;
    }

    @Override
    public Collection<Collection<Protocol.ModifierProtocol>> modifiers()
    {
        return modifiers.stream().map( ModifierProtocolInstaller::protocols ).collect( Collectors.toList() );
    }
}
