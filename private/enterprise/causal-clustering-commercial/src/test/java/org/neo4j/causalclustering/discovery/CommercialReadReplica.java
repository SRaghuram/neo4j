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

import org.neo4j.causalclustering.readreplica.CommercialReadReplicaGraphDatabase;
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
