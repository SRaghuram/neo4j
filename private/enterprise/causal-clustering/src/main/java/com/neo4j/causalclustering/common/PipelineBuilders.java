/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;

import org.neo4j.ssl.config.SslPolicyLoader;

import static org.neo4j.configuration.ssl.SslPolicyScope.BACKUP;
import static org.neo4j.configuration.ssl.SslPolicyScope.CLUSTER;

public final class PipelineBuilders
{
    private final NettyPipelineBuilderFactory clientPipelineBuilderFactory;
    private final NettyPipelineBuilderFactory serverPipelineBuilderFactory;
    private final NettyPipelineBuilderFactory backupServerPipelineBuilderFactory;

    public PipelineBuilders( SslPolicyLoader sslPolicyLoader )
    {
        var clusterSslPolicy = sslPolicyLoader.hasPolicyForSource( CLUSTER ) ? sslPolicyLoader.getPolicy( CLUSTER ) : null;
        var backupSslPolicy = sslPolicyLoader.hasPolicyForSource( BACKUP ) ? sslPolicyLoader.getPolicy( BACKUP ) : null;

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
