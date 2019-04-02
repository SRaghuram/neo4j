/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial.edition;

import java.io.File;
import java.util.function.Function;

import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.AvailabilityGuardInstaller;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

public class CommercialGraphDatabase extends GraphDatabaseFacade
{
    public CommercialGraphDatabase( File storeDir, Config config, GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        new GraphDatabaseFacadeFactory( DatabaseInfo.ENTERPRISE, CommercialEditionModule::new )
                .initFacade( storeDir, config, dependencies, this );
    }

    public CommercialGraphDatabase( File storeDir, Config config, GraphDatabaseFacadeFactory.Dependencies dependencies,
            AvailabilityGuardInstaller guardInstaller )
    {
        Function<PlatformModule,AbstractEditionModule> factory = platform ->
        {
            CommercialEditionModule edition = new CommercialEditionModule( platform );
            AvailabilityGuard guard = edition.getGlobalAvailabilityGuard( platform.clock, platform.logging, platform.config );
            guardInstaller.install( guard );
            return edition;
        };
        new GraphDatabaseFacadeFactory( DatabaseInfo.ENTERPRISE, factory )
                .initFacade( storeDir, config, dependencies, this );
    }
}
