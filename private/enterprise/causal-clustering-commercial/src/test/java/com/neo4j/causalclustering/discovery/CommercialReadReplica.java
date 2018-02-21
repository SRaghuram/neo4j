/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import com.neo4j.causalclustering.readreplica.CommercialReadReplicaGraphDatabase;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.function.IntFunction;

import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.GraphDatabaseDependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.monitoring.Monitors;


public class CommercialReadReplica extends ReadReplica
{
    public CommercialReadReplica( File parentDir, int serverId, int boltPort, int httpPort, int txPort, int backupPort,
                                  DiscoveryServiceFactory discoveryServiceFactory,
                                  List<AdvertisedSocketAddress> coreMemberHazelcastAddresses, Map<String, String> extraParams,
                                  Map<String, IntFunction<String>> instanceExtraParams, String recordFormat, Monitors monitors,
                                  String advertisedAddress, String listenAddress )
    {
        super( parentDir, serverId, boltPort, httpPort, txPort, backupPort, discoveryServiceFactory,
                coreMemberHazelcastAddresses, extraParams, instanceExtraParams, recordFormat, monitors,
                advertisedAddress, listenAddress );
    }

    @Override
    public void start()
    {
        database = new CommercialReadReplicaGraphDatabase( storeDir, Config.defaults( config ),
                GraphDatabaseDependencies.newDependencies().monitors( monitors ), discoveryServiceFactory,
                memberId() );
    }
}
