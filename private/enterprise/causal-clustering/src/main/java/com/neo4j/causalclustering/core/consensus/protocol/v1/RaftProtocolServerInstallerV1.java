/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.protocol.v1;

import com.neo4j.causalclustering.messaging.marshalling.CoreReplicatedContentMarshalFactory;
import com.neo4j.causalclustering.messaging.marshalling.v1.RaftMessageDecoder;
import com.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.Protocol;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;

import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftProtocolServerInstallerV1 implements ProtocolInstaller<ProtocolInstaller.Orientation.Server>
{
    private static final Protocol.ApplicationProtocols APPLICATION_PROTOCOL = Protocol.ApplicationProtocols.RAFT_1;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Server,RaftProtocolServerInstallerV1>
    {
        public Factory( ChannelInboundHandler raftMessageHandler, NettyPipelineBuilderFactory pipelineBuilderFactory,
                String databaseName, LogProvider logProvider )
        {
            super( APPLICATION_PROTOCOL,
                    modifiers -> new RaftProtocolServerInstallerV1( raftMessageHandler, pipelineBuilderFactory, modifiers, databaseName, logProvider ) );
        }
    }

    private final ChannelInboundHandler raftMessageHandler;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final List<ModifierProtocolInstaller<Orientation.Server>> modifiers;
    private final String databaseName;
    private final Log log;

    /**
     * @param databaseName Used when talking raft protocol V1 with a 3.4 instance. It is simply assumed that we have
     * the same database (which is the only supported way of doing a rolling upgrade).
     */
    public RaftProtocolServerInstallerV1( ChannelInboundHandler raftMessageHandler, NettyPipelineBuilderFactory pipelineBuilderFactory,
            List<ModifierProtocolInstaller<Orientation.Server>> modifiers, String databaseName, LogProvider logProvider )
    {
        this.raftMessageHandler = raftMessageHandler;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.modifiers = modifiers;
        this.databaseName = databaseName;
        this.log = logProvider.getLog( getClass() );
    }

    /**
     * Uses latest version of handlers. Hence version naming may be less than the current version if no change was needed for that handler
     */
    @Override
    public void install( Channel channel ) throws Exception
    {
        pipelineBuilderFactory.server( channel, log )
                .modify( modifiers )
                .addFraming()
                .add( "raft_decoder", new RaftMessageDecoder( CoreReplicatedContentMarshalFactory.marshalV1( databaseName ), Clock.systemUTC() ) )
                .add( "raft_handler", raftMessageHandler )
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
        return modifiers.stream()
                .map( ModifierProtocolInstaller::protocols )
                .collect( Collectors.toList() );
    }
}
