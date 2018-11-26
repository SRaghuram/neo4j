/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.discovery.SslDiscoveryServiceFactory;

import java.io.File;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.EnterpriseCoreEditionModule;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.DatabaseInfo;

/**
 * The commercial core database extends the database with functionality only available
 * in the commercial edition, e.g. encrypted network communication, multi-database support, ...
 */
public class CommercialCoreGraphDatabase extends CoreGraphDatabase
{
    public CommercialCoreGraphDatabase( File storeDir, Config config,
                                        GraphDatabaseFacadeFactory.Dependencies dependencies,
                                        SslDiscoveryServiceFactory discoveryServiceFactory )
    {
        super( storeDir, config, dependencies, discoveryServiceFactory );
    }

    @Override
    protected EnterpriseCoreEditionModule cachingFactory( PlatformModule platformModule, DiscoveryServiceFactory discoveryServiceFactory )
    {
        if ( editionModule == null )
        {
            editionModule = new CommercialCoreEditionModule( platformModule, discoveryServiceFactory );
        }
        return editionModule;
    }
}
