/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.internal;

import com.neo4j.server.database.CommercialGraphFactory;
import com.neo4j.server.enterprise.CommercialNeoServer;

import java.io.File;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactorySelector;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory;
import org.neo4j.harness.internal.EnterpriseInProcessServerBuilder;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.AbstractNeoServer;
import org.neo4j.server.database.GraphFactory;

public class CommercialInProcessServerBuilder extends EnterpriseInProcessServerBuilder
{
    private DiscoveryServiceFactorySelector.DiscoveryImplementation discoveryServiceFactory = DiscoveryServiceFactorySelector.DEFAULT;

    public CommercialInProcessServerBuilder( File workingDir, String dataSubDir )
    {
        super( workingDir, dataSubDir );
    }

    @Override
    protected AbstractNeoServer createNeoServer( GraphFactory graphFactory, Config config, GraphDatabaseFacadeFactory.Dependencies dependencies )
    {
        config.augment( CausalClusteringSettings.discovery_implementation, discoveryServiceFactory.name() );
        return new CommercialNeoServer( config, graphFactory, dependencies );
    }

    @Override
    protected GraphFactory createGraphFactory( Config config )
    {
        return new CommercialGraphFactory();
    }
}
