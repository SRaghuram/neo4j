/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.discovery.SslDiscoveryServiceFactory;

import java.io.File;
import java.util.UUID;
import java.util.function.Function;

import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory.Dependencies;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.EditionModule;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;

public class CommercialReadReplicaGraphDatabase extends ReadReplicaGraphDatabase
{
    public CommercialReadReplicaGraphDatabase( File storeDir, Config config, Dependencies dependencies, SslDiscoveryServiceFactory discoveryServiceFactory )
    {
        this( storeDir, config, dependencies, discoveryServiceFactory, new MemberId( UUID.randomUUID() ) );
    }

    public CommercialReadReplicaGraphDatabase( File storeDir, Config config, Dependencies dependencies,
                                               SslDiscoveryServiceFactory discoveryServiceFactory, MemberId memberId )
    {
        Function<PlatformModule,EditionModule> factory =
                platformModule -> new CommercialReadReplicaEditionModule( platformModule,
                        discoveryServiceFactory, memberId );
        new GraphDatabaseFacadeFactory( DatabaseInfo.READ_REPLICA, factory ).initFacade( storeDir, config,
                dependencies, this );
    }
}
