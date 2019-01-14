/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.readreplica.CommercialReadReplicaGraphDatabase;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.graphdb.facade.GraphDatabaseDependencies;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.monitoring.Monitors;

public class CommercialReadReplica extends ReadReplica
{
    private final SslDiscoveryServiceFactory discoveryServiceFactory;

    public CommercialReadReplica( File parentDir, int serverId, int boltPort, int httpPort, int txPort, int backupPort, int discoveryPort,
            SslDiscoveryServiceFactory discoveryServiceFactory,
            List<AdvertisedSocketAddress> coreMemberDiscoveryAddresses, Map<String,String> extraParams,
            Map<String,IntFunction<String>> instanceExtraParams, String recordFormat, Monitors monitors,
            String advertisedAddress, String listenAddress )
    {
        super( parentDir, serverId, boltPort, httpPort, txPort, backupPort, discoveryPort, discoveryServiceFactory,
                coreMemberDiscoveryAddresses, extraParams, instanceExtraParams, recordFormat, monitors,
                advertisedAddress, listenAddress );
        this.discoveryServiceFactory = discoveryServiceFactory;
    }

    @Override
    public void start()
    {
        database = new CommercialReadReplicaGraphDatabase( databasesDirectory, config(),
                GraphDatabaseDependencies.newDependencies().monitors( monitors ), discoveryServiceFactory,
                memberId() );
    }
}
