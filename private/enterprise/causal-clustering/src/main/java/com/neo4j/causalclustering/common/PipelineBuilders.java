/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;

import org.neo4j.configuration.Config;
import org.neo4j.ssl.config.SslPolicyLoader;

public final class PipelineBuilders
{
    private final NettyPipelineBuilderFactory clientPipelineBuilderFactory;
    private final NettyPipelineBuilderFactory serverPipelineBuilderFactory;
    private final NettyPipelineBuilderFactory backupServerPipelineBuilderFactory;

    public PipelineBuilders( Config config, SslPolicyLoader sslPolicyLoader )
    {
        var clusterSslPolicyName = config.get( CausalClusteringSettings.ssl_policy );
        var backupSslPolicyName = config.get( OnlineBackupSettings.ssl_policy );

        var clusterSslPolicy = sslPolicyLoader.getPolicy( clusterSslPolicyName );
        var backupSslPolicy = sslPolicyLoader.getPolicy( backupSslPolicyName );

        clientPipelineBuilderFactory = new NettyPipelineBuilderFactory( clusterSslPolicy );
        serverPipelineBuilderFactory = new NettyPipelineBuilderFactory( clusterSslPolicy );
        backupServerPipelineBuilderFactory = new NettyPipelineBuilderFactory( backupSslPolicy );
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
