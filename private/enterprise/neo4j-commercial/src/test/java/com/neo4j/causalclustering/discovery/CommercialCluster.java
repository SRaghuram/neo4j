/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import org.neo4j.causalclustering.common.Cluster;
import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.causalclustering.discovery.IpFamily;
import org.neo4j.causalclustering.readreplica.ReadReplica;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.ports.allocation.PortAuthority;

public class CommercialCluster extends Cluster<SslDiscoveryServiceFactory>
{
    public CommercialCluster( File parentDir, int noOfCoreMembers, int noOfReadReplicas,
                             SslDiscoveryServiceFactory discoveryServiceFactory,
                             Map<String,String> coreParams, Map<String,IntFunction<String>> instanceCoreParams,
                             Map<String,String> readReplicaParams, Map<String,IntFunction<String>> instanceReadReplicaParams,
                             String recordFormat, IpFamily ipFamily, boolean useWildcard )
    {
        super( parentDir, noOfCoreMembers, noOfReadReplicas, discoveryServiceFactory, coreParams, instanceCoreParams,
                readReplicaParams, instanceReadReplicaParams, recordFormat, ipFamily, useWildcard );
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

        return new CommercialCoreClusterMember(
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
                advertisedAddress
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

        return new CommercialReadReplica(
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
                listenAddress
        );
    }
}
