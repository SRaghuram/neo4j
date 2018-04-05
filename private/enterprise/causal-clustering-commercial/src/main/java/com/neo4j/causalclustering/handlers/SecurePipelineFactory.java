/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.handlers;

import org.neo4j.causalclustering.handlers.DuplexPipelineWrapperFactory;
import org.neo4j.causalclustering.handlers.PipelineWrapper;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.SslPolicy;

public class SecurePipelineFactory implements DuplexPipelineWrapperFactory
{
    public PipelineWrapper forServer( Config config, Dependencies dependencies, LogProvider logProvider, Setting<String> policyName )
    {
        SslPolicy policy = getSslPolicy( config, dependencies, policyName );
        return new SslServerPipelineWrapper( policy );
    }

    @Override
    public PipelineWrapper forClient( Config config, Dependencies dependencies, LogProvider logProvider, Setting<String> policyName )
    {
        SslPolicy policy = getSslPolicy( config, dependencies, policyName );
        return new SslClientPipelineWrapper( policy );
    }

    private SslPolicy getSslPolicy( Config config, Dependencies dependencies, Setting<String> policyNameSetting )
    {
        SslPolicyLoader sslPolicyLoader = dependencies.resolveDependency( SslPolicyLoader.class );
        String policyName = config.get( policyNameSetting );
        return sslPolicyLoader.getPolicy( policyName );
    }
}
