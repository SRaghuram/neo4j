/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.readreplica;

import java.io.File;
import java.util.UUID;
import java.util.function.Function;

import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory.Dependencies;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public class ReadReplicaGraphDatabase extends GraphDatabaseFacade
{
    protected ReadReplicaGraphDatabase()
    {
    }

    public ReadReplicaGraphDatabase( File storeDir, Config config, Dependencies dependencies, DiscoveryServiceFactory discoveryServiceFactory )
    {
        this( storeDir, config, dependencies, discoveryServiceFactory, new MemberId( UUID.randomUUID() ) );
    }

    public ReadReplicaGraphDatabase( File storeDir, Config config, Dependencies dependencies,
            DiscoveryServiceFactory discoveryServiceFactory, MemberId memberId )
    {
        Function<PlatformModule,AbstractEditionModule> factory =
                platformModule -> new EnterpriseReadReplicaEditionModule( platformModule,
                        discoveryServiceFactory, memberId );
        new GraphDatabaseFacadeFactory( DatabaseInfo.READ_REPLICA, factory ).initFacade( storeDir, config,
                dependencies, this );
    }
}
