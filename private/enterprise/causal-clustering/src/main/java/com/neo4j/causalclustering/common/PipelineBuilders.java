/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.handlers.DuplexPipelineWrapperFactory;
import com.neo4j.causalclustering.handlers.PipelineWrapper;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import java.util.function.Supplier;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.LogProvider;

public final class PipelineBuilders
{
    private final NettyPipelineBuilderFactory clientPipelineBuilderFactory;
    private final NettyPipelineBuilderFactory serverPipelineBuilderFactory;
    private final NettyPipelineBuilderFactory backupServerPipelineBuilderFactory;

    public PipelineBuilders( Supplier<DuplexPipelineWrapperFactory> pipelineWrapperFactory, LogProvider logProvider, Config config, Dependencies deps )
    {
        PipelineWrapper serverPipelineWrapper = pipelineWrapperFactory.get().forServer( config, deps, logProvider, CausalClusteringSettings.ssl_policy );
        PipelineWrapper clientPipelineWrapper = pipelineWrapperFactory.get().forClient( config, deps, logProvider, CausalClusteringSettings.ssl_policy );
        PipelineWrapper backupServerPipelineWrapper = pipelineWrapperFactory.get().forServer( config, deps, logProvider, OnlineBackupSettings.ssl_policy );

        clientPipelineBuilderFactory = new NettyPipelineBuilderFactory( clientPipelineWrapper );
        serverPipelineBuilderFactory = new NettyPipelineBuilderFactory( serverPipelineWrapper );
        backupServerPipelineBuilderFactory = new NettyPipelineBuilderFactory( backupServerPipelineWrapper );
    }

    public NettyPipelineBuilderFactory client()
    {
        return clientPipelineBuilderFactory;
    }

    public NettyPipelineBuilderFactory server()
    {
        return serverPipelineBuilderFactory;
    }

    public NettyPipelineBuilderFactory backupServer()
    {
        return backupServerPipelineBuilderFactory;
    }
}
