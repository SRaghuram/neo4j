/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
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
import org.neo4j.kernel.monitoring.Monitors;

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
                                        String advertisedAddress)
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
