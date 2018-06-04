/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import java.io.File;
import java.util.UUID;
import java.util.function.Function;

import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.HazelcastDiscoveryServiceFactory;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Dependencies;
import org.neo4j.kernel.impl.factory.PlatformModule;

public class CommercialReadReplicaGraphDatabase extends ReadReplicaGraphDatabase
{
    public CommercialReadReplicaGraphDatabase( File storeDir, Config config, Dependencies dependencies )
    {
        this( storeDir, config, dependencies, new HazelcastDiscoveryServiceFactory(), new MemberId( UUID.randomUUID() ) );
    }

    public CommercialReadReplicaGraphDatabase( File storeDir, Config config, Dependencies dependencies,
                                               DiscoveryServiceFactory discoveryServiceFactory, MemberId memberId )
    {
        Function<PlatformModule,EditionModule> factory =
                ( platformModule ) -> new CommercialReadReplicaEditionModule( platformModule,
                        discoveryServiceFactory, memberId );
        new GraphDatabaseFacadeFactory( DatabaseInfo.READ_REPLICA, factory ).initFacade( storeDir, config,
                dependencies, this );
    }
}
