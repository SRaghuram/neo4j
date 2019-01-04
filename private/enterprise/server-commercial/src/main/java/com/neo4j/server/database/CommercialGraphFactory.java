/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.database;

import com.neo4j.causalclustering.core.CommercialCoreGraphDatabase;
import com.neo4j.causalclustering.discovery.CommercialDiscoveryServiceFactorySelector;
import com.neo4j.causalclustering.discovery.SslDiscoveryServiceFactory;
import com.neo4j.causalclustering.readreplica.CommercialReadReplicaGraphDatabase;
import com.neo4j.commercial.edition.CommercialGraphDatabase;
import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;

import java.io.File;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.server.database.GraphFactory;

import static org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory.Dependencies;

public class CommercialGraphFactory implements GraphFactory
{
    @Override
    public GraphDatabaseFacade newGraphDatabase( Config config, Dependencies dependencies )
    {
        EnterpriseEditionSettings.Mode mode = config.get( EnterpriseEditionSettings.mode );
        File storeDir = config.get( GraphDatabaseSettings.databases_root_path );
        SslDiscoveryServiceFactory discoveryServiceFactory = new CommercialDiscoveryServiceFactorySelector().select( config );

        switch ( mode )
        {
        case CORE:
            return new CommercialCoreGraphDatabase( storeDir, config, dependencies, discoveryServiceFactory );
        case READ_REPLICA:
            return new CommercialReadReplicaGraphDatabase( storeDir, config, dependencies, discoveryServiceFactory );
        default:
            return new CommercialGraphDatabase( storeDir, config, dependencies );
        }
    }
}
