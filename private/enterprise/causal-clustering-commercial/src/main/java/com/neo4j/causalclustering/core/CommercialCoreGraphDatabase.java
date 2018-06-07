/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.discovery.SslHazelcastDiscoveryServiceFactory;

import java.io.File;
import java.util.function.Function;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.EditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory;
import org.neo4j.kernel.impl.factory.PlatformModule;

public class CommercialCoreGraphDatabase extends CoreGraphDatabase
{
    public CommercialCoreGraphDatabase( File storeDir, Config config,
                                        GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        this( storeDir, config, dependencies, new SslHazelcastDiscoveryServiceFactory() );
    }

    public CommercialCoreGraphDatabase( File storeDir, Config config,
                                        GraphDatabaseFacadeFactory.Dependencies dependencies,
                                        DiscoveryServiceFactory discoveryServiceFactory )
    {
        Function<PlatformModule,EditionModule> factory =
                platformModule -> new CommercialCoreEditionModule( platformModule, discoveryServiceFactory );
        new GraphDatabaseFacadeFactory( DatabaseInfo.CORE, factory ).initFacade( storeDir, config, dependencies, this );
    }
}
