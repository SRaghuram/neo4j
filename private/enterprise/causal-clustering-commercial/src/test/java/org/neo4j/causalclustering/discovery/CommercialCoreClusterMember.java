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

import org.neo4j.causalclustering.core.CommercialCoreGraphDatabase;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class CommercialCoreClusterMember extends CoreClusterMember
{
    private final Map<String, String> config = stringMap();

    public CommercialCoreClusterMember( int serverId,
                                        int discoveryPort,
                                        int txPort,
                                        int raftPort,
                                        int boltPort,
                                        int httpPort,
                                        int backupPort,
                                        int clusterSize,
                                        List<AdvertisedSocketAddress> addresses,
                                        DiscoveryServiceFactory discoveryServiceFactory,
                                        String recordFormat,
                                        File parentDir,
                                        Map<String, String> extraParams,
                                        Map<String, IntFunction<String>> instanceExtraParams,
                                        String listenAddress,
                                        String advertisedAddress )
    {
        super( serverId, discoveryPort, txPort, raftPort, boltPort, httpPort, backupPort, clusterSize, addresses,
                discoveryServiceFactory, recordFormat, parentDir, extraParams, instanceExtraParams, listenAddress,
                advertisedAddress );
    }

    @Override
    public void start()
    {
        database = new CommercialCoreGraphDatabase( storeDir, Config.defaults( config ),
                GraphDatabaseDependencies.newDependencies(), discoveryServiceFactory );
    }
}
