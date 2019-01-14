/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.core.CommercialCoreGraphDatabase;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import org.neo4j.causalclustering.core.CoreClusterMember;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.helpers.AdvertisedSocketAddress;

public class CommercialCoreClusterMember extends CoreClusterMember
{
    private final SslDiscoveryServiceFactory discoveryServiceFactory;

    CommercialCoreClusterMember( int serverId,
                                        int discoveryPort,
                                        int txPort,
                                        int raftPort,
                                        int boltPort,
                                        int httpPort,
                                        int backupPort,
                                        int clusterSize,
                                        List<AdvertisedSocketAddress> addresses,
                                        SslDiscoveryServiceFactory discoveryServiceFactory,
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
        this.discoveryServiceFactory = discoveryServiceFactory;
    }

    @Override
    public void start()
    {
        database = new CommercialCoreGraphDatabase( databasesDirectory(), config(),
                GraphDatabaseDependencies.newDependencies(), discoveryServiceFactory );
    }
}
