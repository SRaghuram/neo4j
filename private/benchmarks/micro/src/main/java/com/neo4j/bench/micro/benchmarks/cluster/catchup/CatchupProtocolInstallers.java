/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.micro.benchmarks.cluster.catchup;

import com.neo4j.bench.micro.benchmarks.cluster.ProtocolInstallers;
import com.neo4j.causalclustering.catchup.CatchupResponseHandler;
import com.neo4j.causalclustering.catchup.CatchupServerHandler;
import com.neo4j.causalclustering.catchup.v3.CatchupProtocolClientInstallerV3;
import com.neo4j.causalclustering.catchup.v3.CatchupProtocolServerInstallerV3;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.causalclustering.protocol.ProtocolInstaller;

import org.neo4j.logging.LogProvider;
import org.neo4j.storageengine.api.CommandReaderFactory;

import static java.util.Collections.emptyList;

class CatchupProtocolInstallers implements ProtocolInstallers
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
        return new CatchupProtocolClientInstallerV3( pipelineBuilderFactory, emptyList(), logProvider, responseHandler, commandReaderFactory );
    }

    @Override
    public ProtocolInstaller<ProtocolInstaller.Orientation.Server> serverInstaller()
    {
        return new CatchupProtocolServerInstallerV3( pipelineBuilderFactory, emptyList(), logProvider, serverHandler );
    }
}
