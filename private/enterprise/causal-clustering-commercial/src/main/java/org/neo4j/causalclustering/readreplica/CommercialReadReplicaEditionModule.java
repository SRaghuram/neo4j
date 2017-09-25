/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.readreplica;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.SslHazelcastDiscoveryServiceFactory;
import org.neo4j.causalclustering.handlers.PipelineHandlerAppenderFactory;
import org.neo4j.causalclustering.handlers.SslPipelineHandlerAppenderFactory;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.PlatformModule;
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
        SslPolicy clusterSslPolicy = sslPolicyFactory.getPolicy( config.get( CausalClusteringSettings
                .ssl_policy ) );

        if ( discoveryServiceFactory instanceof SslHazelcastDiscoveryServiceFactory )
        {
            ((SslHazelcastDiscoveryServiceFactory) discoveryServiceFactory).setSslPolicy( clusterSslPolicy );
        }
    }

    @Override
    protected PipelineHandlerAppenderFactory appenderFactory()
    {
        return new SslPipelineHandlerAppenderFactory();
    }
}
