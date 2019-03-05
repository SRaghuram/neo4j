/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.handlers;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.config.SslPolicyLoader;

public class SecurePipelineFactory implements DuplexPipelineWrapperFactory
{
    @Override
    public PipelineWrapper forServer( Config config, Setting<String> policyName, SslPolicyLoader sslPolicyLoader )
    {
        SslPolicy policy = getSslPolicy( config, policyName, sslPolicyLoader );
        return new SslServerPipelineWrapper( policy );
    }

    @Override
    public PipelineWrapper forClient( Config config, Setting<String> policyName, SslPolicyLoader sslPolicyLoader )
    {
        SslPolicy policy = getSslPolicy( config, policyName, sslPolicyLoader );
        return new SslClientPipelineWrapper( policy );
    }

    private static SslPolicy getSslPolicy( Config config, Setting<String> policyNameSetting, SslPolicyLoader sslPolicyLoader )
    {
        String policyName = config.get( policyNameSetting );
        return sslPolicyLoader.getPolicy( policyName );
    }
}
