/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.handlers;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.SslPolicy;

public class SslPipelineHandlerAppenderFactory implements PipelineHandlerAppenderFactory
{
    public PipelineHandlerAppender create( Config config, Dependencies dependencies, LogProvider logProvider )
    {
        SslPolicyLoader sslPolicyLoader = dependencies.resolveDependency( SslPolicyLoader.class );
        String policyName = config.get( CausalClusteringSettings.ssl_policy );
        SslPolicy policy = sslPolicyLoader.getPolicy( policyName );
        return new SslPipelineHandlerAppender( policy );
    }
}
