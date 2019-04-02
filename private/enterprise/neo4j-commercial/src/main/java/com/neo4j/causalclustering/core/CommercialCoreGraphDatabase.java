/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.discovery.SslDiscoveryServiceFactory;

import java.io.File;
import java.util.function.Function;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityGuardInstaller;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;

public class CommercialCoreGraphDatabase extends CoreGraphDatabase
{
    public CommercialCoreGraphDatabase( File storeDir, Config config,
                                        GraphDatabaseFacadeFactory.Dependencies dependencies,
                                        SslDiscoveryServiceFactory discoveryServiceFactory )
    {
        this( storeDir, config, dependencies, discoveryServiceFactory, availabilityGuard -> {} );
    }

    public CommercialCoreGraphDatabase( File storeDir, Config config, GraphDatabaseFacadeFactory.Dependencies dependencies,
            SslDiscoveryServiceFactory discoveryServiceFactory, AvailabilityGuardInstaller guardInstaller )
    {
        Function<PlatformModule,AbstractEditionModule> factory = platformModule ->
        {
            CommercialCoreEditionModule edition = new CommercialCoreEditionModule( platformModule, discoveryServiceFactory );
            AvailabilityGuard guard = edition.getGlobalAvailabilityGuard( platformModule.clock, platformModule.logging, platformModule.config );
            guardInstaller.install( guard );
            return edition;
        };
        new GraphDatabaseFacadeFactory( DatabaseInfo.CORE, factory ).initFacade( storeDir, config, dependencies, this );
    }
}
