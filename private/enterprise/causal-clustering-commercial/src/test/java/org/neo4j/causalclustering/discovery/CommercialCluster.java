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
package org.neo4j.causalclustering.discovery;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import org.neo4j.com.ports.allocation.PortAuthority;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.monitoring.Monitors;

public class CommercialCluster extends Cluster
{
    public CommercialCluster( File parentDir, int noOfCoreMembers, int noOfReadReplicas,
                              DiscoveryServiceFactory discoveryServiceFactory,
                              Map<String,String> coreParams, Map<String,IntFunction<String>> instanceCoreParams,
                              Map<String,String> readReplicaParams, Map<String,IntFunction<String>> instanceReadReplicaParams,
                              String recordFormat, IpFamily ipFamily, boolean useWildcard )
    {
        super( parentDir, noOfCoreMembers, noOfReadReplicas, discoveryServiceFactory, coreParams, instanceCoreParams,
                readReplicaParams, instanceReadReplicaParams, recordFormat, ipFamily, useWildcard );
    }

    protected CoreClusterMember createCoreClusterMember( int serverId,
                                                       int hazelcastPort,
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
                hazelcastPort,
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

    private ReadReplica createReadReplica( int serverId,
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

        return new CommercialReadReplica(
                parentDir,
                serverId,
                boltPort,
                httpPort,
                txPort,
                backupPort, discoveryServiceFactory,
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
