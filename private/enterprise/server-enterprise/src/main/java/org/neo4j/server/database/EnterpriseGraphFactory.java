/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.database;

import java.io.File;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.discovery.EnterpriseDiscoveryServiceFactorySelector;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.enterprise.EnterpriseGraphDatabase;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;

import static org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory.Dependencies;

public class EnterpriseGraphFactory implements GraphFactory
{
    @Override
    public GraphDatabaseFacade newGraphDatabase( Config config, Dependencies dependencies )
    {
        EnterpriseEditionSettings.Mode mode = config.get( EnterpriseEditionSettings.mode );
        File storeDir = config.get( GraphDatabaseSettings.databases_root_path );
        DiscoveryServiceFactory discoveryServiceFactory = new EnterpriseDiscoveryServiceFactorySelector().select( config );

        switch ( mode )
        {
        case CORE:
            return new CoreGraphDatabase( storeDir, config, dependencies, discoveryServiceFactory );
        case READ_REPLICA:
            return new ReadReplicaGraphDatabase( storeDir, config, dependencies, discoveryServiceFactory );
        default:
            return new EnterpriseGraphDatabase( storeDir, config, dependencies );
        }
    }
}
