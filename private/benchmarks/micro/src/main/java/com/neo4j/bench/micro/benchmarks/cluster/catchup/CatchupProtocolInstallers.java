/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import com.neo4j.bench.micro.benchmarks.cluster.ProtocolInstallers;
import com.neo4j.bench.micro.benchmarks.cluster.ProtocolVersion;
import com.neo4j.causalclustering.catchup.CatchupResponseHandler;
import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.catchup.v3.CatchupProtocolClientInstaller;
import com.neo4j.causalclustering.catchup.v3.CatchupProtocolServerInstaller;
import com.neo4j.causalclustering.core.consensus.RaftMessages;
import com.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolClientInstallerV2;
import com.neo4j.causalclustering.messaging.Inbound;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;

import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.CommandReaderFactory;

import static java.util.Collections.emptyList;

public class CatchupProtocolInstallers implements ProtocolInstallers
{
    private final LogProvider logProvider;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final CatchupResponseHandler responseHandler;
    private final CatchupServerHandler serverHandler;
    private final CommandReaderFactory commandReaderFactory;

    CatchupProtocolInstallers( LogProvider logProvider, CatchupResponseHandler responseHandler, CommandReaderFactory commandReaderFactory,
            CatchupServerHandler serverHandler )
    {
        this.logProvider = logProvider;
        this.pipelineBuilderFactory = NettyPipelineBuilderFactory.insecure();
        this.responseHandler = responseHandler;
        this.serverHandler = serverHandler;
        this.commandReaderFactory = commandReaderFactory;
    }

    @Override
    public ProtocolInstaller<ProtocolInstaller.Orientation.Client> clientInstaller()
    {
        return new CatchupProtocolClientInstaller( pipelineBuilderFactory, emptyList(), logProvider, responseHandler, commandReaderFactory );
    }

    @Override
    public ProtocolInstaller<ProtocolInstaller.Orientation.Server> serverInstaller()
    {
        return new CatchupProtocolServerInstaller( pipelineBuilderFactory, emptyList(), logProvider, serverHandler );
    }
}
