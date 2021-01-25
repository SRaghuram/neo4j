/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import com.neo4j.bench.micro.benchmarks.cluster.ProtocolInstallers;
import com.neo4j.causalclustering.catchup.CatchupInboundEventListener;
import com.neo4j.causalclustering.catchup.CatchupResponseHandler;
import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.catchup.v3.CatchupProtocolClientInstallerV3;
import com.neo4j.causalclustering.catchup.v3.CatchupProtocolServerInstallerV3;
import com.neo4j.causalclustering.catchup.v4.CatchupProtocolClientInstallerV4;
import com.neo4j.causalclustering.catchup.v4.CatchupProtocolServerInstallerV4;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;

import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.CommandReaderFactory;

import static java.util.Collections.emptyList;

class CatchupProtocolInstallers implements ProtocolInstallers
{
    private final ProtocolVersion version;
    private final LogProvider logProvider;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final CatchupResponseHandler responseHandler;
    private final CatchupServerHandler serverHandler;
    private final CommandReaderFactory commandReaderFactory;

    CatchupProtocolInstallers( ProtocolVersion version, LogProvider logProvider, CatchupResponseHandler responseHandler,
            CommandReaderFactory commandReaderFactory, CatchupServerHandler serverHandler )
    {
        this.version = version;
        this.logProvider = logProvider;
        this.pipelineBuilderFactory = NettyPipelineBuilderFactory.insecure();
        this.responseHandler = responseHandler;
        this.serverHandler = serverHandler;
        this.commandReaderFactory = commandReaderFactory;
    }

    @Override
    public ProtocolInstaller<ProtocolInstaller.Orientation.Client> clientInstaller()
    {
        switch ( version )
        {
        case V3:
            return new CatchupProtocolClientInstallerV3( pipelineBuilderFactory, emptyList(), logProvider, responseHandler, commandReaderFactory );
        case V4:
        case LATEST:
            return new CatchupProtocolClientInstallerV4( pipelineBuilderFactory, emptyList(), logProvider, responseHandler, commandReaderFactory );
        default:
            throw new IllegalArgumentException( "Can't handle: " + version );
        }
    }

    @Override
    public ProtocolInstaller<ProtocolInstaller.Orientation.Server> serverInstaller()
    {
        switch ( version )
        {
        case V3:
            return new CatchupProtocolServerInstallerV3( pipelineBuilderFactory, emptyList(), logProvider, serverHandler, CatchupInboundEventListener.NO_OP );
        case V4:
        case LATEST:
            return new CatchupProtocolServerInstallerV4( pipelineBuilderFactory, emptyList(), logProvider, serverHandler, CatchupInboundEventListener.NO_OP );
        default:
            throw new IllegalArgumentException( "Can't handle: " + version );
        }
    }
}
