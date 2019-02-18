/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.hazelcast.config.NetworkConfig;
import com.neo4j.causalclustering.identity.MemberId;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.SslPolicy;

class SslHazelcastCoreTopologyService extends HazelcastCoreTopologyService
{
    private final SslPolicy sslPolicy;

    SslHazelcastCoreTopologyService( Config config, SslPolicy sslPolicy, MemberId myself, JobScheduler jobScheduler,
                                     LogProvider logProvider, LogProvider userLogProvider, RemoteMembersResolver remoteMembersResolver,
                                     RetryStrategy topologyServiceRetryStrategy, Monitors monitors )
    {
        super( config, myself, jobScheduler, logProvider, userLogProvider, remoteMembersResolver, topologyServiceRetryStrategy, monitors );
        this.sslPolicy = sslPolicy;
    }

    protected void additionalConfig( NetworkConfig networkConfig, LogProvider logProvider )
    {
        HazelcastSslConfiguration.configureSsl( networkConfig, sslPolicy, logProvider );
    }
}
