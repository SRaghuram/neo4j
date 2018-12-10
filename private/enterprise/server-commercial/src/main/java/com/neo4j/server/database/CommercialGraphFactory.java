/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.database;

import com.neo4j.causalclustering.core.CoreEditionModule;
import com.neo4j.causalclustering.core.CoreGraphDatabase;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import com.neo4j.causalclustering.discovery.DiscoveryServiceFactorySelector;
import com.neo4j.causalclustering.readreplica.ReadReplicaEditionModule;
import com.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import com.neo4j.commercial.edition.CommercialGraphDatabase;
import com.neo4j.kernel.impl.enterprise.configuration.CommercialEditionSettings;

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
        CommercialEditionSettings.Mode mode = config.get( CommercialEditionSettings.mode );
        File storeDir = config.get( GraphDatabaseSettings.databases_root_path );
        DiscoveryServiceFactory discoveryServiceFactory = new DiscoveryServiceFactorySelector().select( config );

        switch ( mode )
        {
        case CORE:
            return new CoreGraphDatabase( storeDir, config, dependencies, discoveryServiceFactory, CoreEditionModule::new );
        case READ_REPLICA:
            return new ReadReplicaGraphDatabase( storeDir, config, dependencies, discoveryServiceFactory, ReadReplicaEditionModule::new );
        default:
            return new CommercialGraphDatabase( storeDir, config, dependencies );
        }
    }
}
