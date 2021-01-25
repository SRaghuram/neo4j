/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.discovery.member.CoreServerSnapshotFactory;
import com.neo4j.causalclustering.discovery.member.ServerSnapshotFactory;
import com.neo4j.causalclustering.error_handling.Panicker;
import com.neo4j.causalclustering.identity.CoreServerIdentity;

import java.time.Clock;

import org.neo4j.configuration.Config;
import org.neo4j.dbms.DatabaseStateService;
import org.neo4j.dbms.identity.ServerIdentity;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.ssl.config.SslPolicyLoader;

public interface DiscoveryServiceFactory
{
    CoreTopologyService coreTopologyService( Config config, CoreServerIdentity myIdentity, JobScheduler jobScheduler, LogProvider logProvider,
                                             LogProvider userLogProvider, RemoteMembersResolver remoteMembersResolver,
                                             RetryStrategy topologyServiceRetryStrategy,
                                             SslPolicyLoader sslPolicyLoader, CoreServerSnapshotFactory serverSnapshotFactory,
                                             DiscoveryFirstStartupDetector firstStartupDetector, Monitors monitors,
                                             Clock clock, DatabaseStateService databaseStateService, Panicker panicker );

    TopologyService readReplicaTopologyService( Config config, LogProvider logProvider, JobScheduler jobScheduler, ServerIdentity myIdentity,
            RemoteMembersResolver remoteMembersResolver, SslPolicyLoader sslPolicyLoader, ServerSnapshotFactory serverSnapshotFactory, Clock clock,
            DatabaseStateService databaseStateService );
}
