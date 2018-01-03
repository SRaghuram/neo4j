/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.discovery;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.SslPolicy;

public class SslHazelcastDiscoveryServiceFactory extends HazelcastDiscoveryServiceFactory
{
    private SslPolicy sslPolicy;

    @Override
    public CoreTopologyService coreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler,
            LogProvider logProvider, LogProvider userLogProvider, HostnameResolver hostnameResolver,
            TopologyServiceRetryStrategy topologyServiceRetryStrategy )
    {
        configureHazelcast( config, logProvider );
        return new SslHazelcastCoreTopologyService( config, sslPolicy, myself, jobScheduler, logProvider,
                userLogProvider, hostnameResolver, topologyServiceRetryStrategy );
    }

    @Override
    public TopologyService topologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler,
                                            MemberId myself, HostnameResolver hostnameResolver,
                                            TopologyServiceRetryStrategy topologyServiceRetryStrategy )
    {
        configureHazelcast( config, logProvider );
        return new HazelcastClient( new SslHazelcastClientConnector( config, logProvider, sslPolicy, hostnameResolver ),
                jobScheduler, logProvider, config, myself, topologyServiceRetryStrategy );
    }

    public void setSslPolicy( SslPolicy sslPolicy )
    {
        this.sslPolicy = sslPolicy;
    }
}
