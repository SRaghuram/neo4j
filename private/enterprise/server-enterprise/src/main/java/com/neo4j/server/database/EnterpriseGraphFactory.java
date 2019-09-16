/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.database;

import com.neo4j.causalclustering.core.CoreEditionModule;
import com.neo4j.causalclustering.core.CoreGraphDatabase;
import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;
import com.neo4j.causalclustering.readreplica.ReadReplicaEditionModule;
import com.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import com.neo4j.enterprise.edition.EnterpriseEditionModule;
import com.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;

import java.io.File;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.server.database.GraphFactory;

public class EnterpriseGraphFactory implements GraphFactory
{
    @Override
    public DatabaseManagementService newDatabaseManagementService( Config config, ExternalDependencies dependencies )
    {
        EnterpriseEditionSettings.Mode mode = config.get( EnterpriseEditionSettings.mode );
        File storeDir = config.get( GraphDatabaseSettings.databases_root_path ).toFile();

        switch ( mode )
        {
        case CORE:
            return new CoreGraphDatabase( storeDir, config, dependencies, newDiscoveryServiceFactory(), CoreEditionModule::new ).getManagementService();
        case READ_REPLICA:
            return new ReadReplicaGraphDatabase( storeDir, config, dependencies, newDiscoveryServiceFactory(),
                    ReadReplicaEditionModule::new ).getManagementService();
        default:
            return new DatabaseManagementServiceFactory( DatabaseInfo.ENTERPRISE, EnterpriseEditionModule::new )
                    .build( storeDir, config, dependencies );
        }
    }

    private static AkkaDiscoveryServiceFactory newDiscoveryServiceFactory()
    {
        return new AkkaDiscoveryServiceFactory();
    }
}
