/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.core.consensus.protocol.v1;

import io.netty.channel.Channel;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.messaging.marshalling.CoreReplicatedContentMarshal;
import org.neo4j.causalclustering.messaging.marshalling.v1.RaftMessageEncoder;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftProtocolClientInstallerV1 implements ProtocolInstaller<Orientation.Client>
{
    private static final Protocol.ApplicationProtocols APPLICATION_PROTOCOL = Protocol.ApplicationProtocols.RAFT_1;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Client,RaftProtocolClientInstallerV1>
    {
        public Factory( NettyPipelineBuilderFactory clientPipelineBuilderFactory, LogProvider logProvider )
        {
            super( APPLICATION_PROTOCOL,
                    modifiers -> new RaftProtocolClientInstallerV1( clientPipelineBuilderFactory, modifiers, logProvider ) );
        }
    }

    private final List<ModifierProtocolInstaller<Orientation.Client>> modifiers;
    private final Log log;
    private final NettyPipelineBuilderFactory clientPipelineBuilderFactory;

    public RaftProtocolClientInstallerV1( NettyPipelineBuilderFactory clientPipelineBuilderFactory,
            List<ModifierProtocolInstaller<Orientation.Client>> modifiers, LogProvider logProvider )
    {
        this.modifiers = modifiers;
        this.log = logProvider.getLog( getClass() );
        this.clientPipelineBuilderFactory = clientPipelineBuilderFactory;
    }

    @Override
    public void install( Channel channel ) throws Exception
    {
        clientPipelineBuilderFactory.client( channel, log )
                .modify( modifiers )
                .addFraming()
                .add( "raft_encoder", new RaftMessageEncoder( CoreReplicatedContentMarshal.marshaller() ) )
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
