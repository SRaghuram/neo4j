/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.discovery.SslHazelcastDiscoveryServiceFactory;
import com.neo4j.causalclustering.handlers.SecurePipelineFactory;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.handlers.DuplexPipelineWrapperFactory;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.readreplica.EnterpriseReadReplicaEditionModule;
import org.neo4j.graphdb.factory.module.EditionModule;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.SslPolicy;

/**
 * This implementation of {@link EditionModule} creates the implementations of services
 * that are specific to the Enterprise Read Replica edition.
 */
public class CommercialReadReplicaEditionModule extends EnterpriseReadReplicaEditionModule
{
    CommercialReadReplicaEditionModule( final PlatformModule platformModule, final DiscoveryServiceFactory discoveryServiceFactory, MemberId myself )
    {
        super( platformModule, discoveryServiceFactory, myself );
    }

    @Override
    protected void configureDiscoveryService( DiscoveryServiceFactory discoveryServiceFactory, Dependencies dependencies,
                                              Config config, LogProvider logProvider )
    {
        SslPolicyLoader sslPolicyFactory = dependencies.satisfyDependency( SslPolicyLoader.create( config, logProvider ) );
        SslPolicy clusterSslPolicy = sslPolicyFactory.getPolicy( config.get( CausalClusteringSettings.ssl_policy ) );

        if ( discoveryServiceFactory instanceof SslHazelcastDiscoveryServiceFactory )
        {
            ((SslHazelcastDiscoveryServiceFactory) discoveryServiceFactory).setSslPolicy( clusterSslPolicy );
        }
    }

    @Override
    protected DuplexPipelineWrapperFactory pipelineWrapperFactory()
    {
        return new SecurePipelineFactory();
    }
}
