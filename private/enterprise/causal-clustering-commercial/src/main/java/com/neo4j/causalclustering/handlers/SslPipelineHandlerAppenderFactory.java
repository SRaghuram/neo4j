/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.handlers;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.handlers.PipelineHandlerAppender;
import org.neo4j.causalclustering.handlers.PipelineHandlerAppenderFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.LogProvider;

public class SslPipelineHandlerAppenderFactory implements PipelineHandlerAppenderFactory
{
    public PipelineHandlerAppender create( Config config, Dependencies dependencies, LogProvider logProvider )
    {
        return new SslPipelineHandlerAppender( dependencies.provideDependency( SslPolicyLoader.class ).get()
                .getPolicy( config.get( CausalClusteringSettings.ssl_policy ) ) );
    }
}
