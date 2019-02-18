/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.core.CoreClusterMember;
import com.neo4j.causalclustering.core.CoreEditionModule;
import com.neo4j.causalclustering.core.CoreGraphDatabase;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.IpFamily;
import com.neo4j.causalclustering.identity.MemberId;
import com.neo4j.causalclustering.readreplica.ReadReplica;
import com.neo4j.causalclustering.readreplica.ReadReplicaEditionModule;
import com.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntFunction;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.ports.PortAuthority;

public class DefaultCluster extends Cluster<DiscoveryServiceFactory>
{
    public DefaultCluster( File parentDir, int noOfCoreMembers, int noOfReadReplicas, DiscoveryServiceFactory discoveryServiceFactory,
            Map<String,String> coreParams, Map<String,IntFunction<String>> instanceCoreParams, Map<String,String> readReplicaParams,
            Map<String,IntFunction<String>> instanceReadReplicaParams, String recordFormat, IpFamily ipFamily, boolean useWildcard )
    {
        super( parentDir, noOfCoreMembers, noOfReadReplicas, discoveryServiceFactory, coreParams, instanceCoreParams, readReplicaParams,
                instanceReadReplicaParams, recordFormat, ipFamily, useWildcard );
    }

    public DefaultCluster( File parentDir, int noOfCoreMembers, int noOfReadReplicas, DiscoveryServiceFactory discoveryServiceFactory,
            Map<String,String> coreParams, Map<String,IntFunction<String>> instanceCoreParams, Map<String,String> readReplicaParams,
            Map<String,IntFunction<String>> instanceReadReplicaParams, String recordFormat, IpFamily ipFamily, boolean useWildcard, Set<String> dbNames )
    {
        super( parentDir, noOfCoreMembers, noOfReadReplicas, discoveryServiceFactory, coreParams, instanceCoreParams, readReplicaParams,
                instanceReadReplicaParams, recordFormat, ipFamily, useWildcard, dbNames );
    }

    @Override
    protected CoreClusterMember createCoreClusterMember( int serverId,
            int discoveryPort,
            int clusterSize,
            List<AdvertisedSocketAddress> initialHosts,
            String recordFormat,
            Map<String, String> extraParams,
            Map<String, IntFunction<String>> instanceExtraParams )
    {
        int txPort = PortAuthority.allocatePort();
        int raftPort = PortAuthority.allocatePort();
        int boltPort = PortAuthority.allocatePort();
        int httpPort = PortAuthority.allocatePort();
        int backupPort = PortAuthority.allocatePort();

        return new CoreClusterMember(
                serverId,
                discoveryPort,
                txPort,
                raftPort,
                boltPort,
                httpPort,
                backupPort,
                clusterSize,
                initialHosts,
                discoveryServiceFactory,
                recordFormat,
                parentDir,
                extraParams,
                instanceExtraParams,
                listenAddress,
                advertisedAddress,
                ( File file, Config config, GraphDatabaseDependencies dependencies, DiscoveryServiceFactory discoveryServiceFactory ) ->
                        new CoreGraphDatabase( file, config, dependencies, discoveryServiceFactory, CoreEditionModule::new )
        );
    }

    @Override
    protected ReadReplica createReadReplica( int serverId,
            List<AdvertisedSocketAddress> initialHosts,
            Map<String, String> extraParams,
            Map<String, IntFunction<String>> instanceExtraParams,
            String recordFormat,
            Monitors monitors )
    {
        int boltPort = PortAuthority.allocatePort();
        int httpPort = PortAuthority.allocatePort();
        int txPort = PortAuthority.allocatePort();
        int backupPort = PortAuthority.allocatePort();
        int discoveryPort = PortAuthority.allocatePort();

        return new ReadReplica(
                parentDir,
                serverId,
                boltPort,
                httpPort,
                txPort,
                backupPort,
                discoveryPort,
                discoveryServiceFactory,
                initialHosts,
                extraParams,
                instanceExtraParams,
                recordFormat,
                monitors,
                advertisedAddress,
                listenAddress,
                ( File file, Config config, GraphDatabaseDependencies dependencies, DiscoveryServiceFactory discoveryServiceFactory, MemberId memberId ) ->
                        new ReadReplicaGraphDatabase( file, config, dependencies, discoveryServiceFactory, memberId, ReadReplicaEditionModule::new )
        );
    }
}
