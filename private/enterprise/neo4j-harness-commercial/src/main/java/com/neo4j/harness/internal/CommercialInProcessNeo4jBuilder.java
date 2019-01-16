/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.internal;

import com.neo4j.causalclustering.discovery.DiscoveryImplementation;
import com.neo4j.server.database.CommercialGraphFactory;
import com.neo4j.server.enterprise.CommercialNeoServer;
import org.apache.commons.lang3.SystemUtils;

import java.io.File;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactorySelector;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory.Dependencies;
import org.neo4j.harness.internal.AbstractInProcessNeo4jBuilder;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.server.AbstractNeoServer;
import org.neo4j.server.database.GraphFactory;

public class CommercialInProcessNeo4jBuilder extends AbstractInProcessNeo4jBuilder
{
    private DiscoveryImplementation discoveryServiceFactory = DiscoveryServiceFactorySelector.DEFAULT;

    public CommercialInProcessNeo4jBuilder()
    {
        this( SystemUtils.getJavaIoTmpDir() );
    }

    public CommercialInProcessNeo4jBuilder( File workingDir )
    {
        super( workingDir );
    }

    public CommercialInProcessNeo4jBuilder( File workingDir, String dataSubDir )
    {
        super( workingDir, dataSubDir );
    }

    @Override
    protected GraphFactory createGraphFactory( Config config )
    {
        return new CommercialGraphFactory();
    }

    @Override
    protected AbstractNeoServer createNeoServer( GraphFactory graphFactory, Config config, Dependencies dependencies )
    {
        config.augment( CausalClusteringSettings.discovery_implementation, discoveryServiceFactory.name() );
        return new CommercialNeoServer( config, graphFactory, dependencies );
    }
}
