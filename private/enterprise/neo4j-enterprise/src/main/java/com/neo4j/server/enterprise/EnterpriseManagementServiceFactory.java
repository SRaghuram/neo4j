/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.causalclustering.core.CoreEditionModule;
import com.neo4j.causalclustering.core.CoreGraphDatabase;
import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;
import com.neo4j.causalclustering.readreplica.ReadReplicaEditionModule;
import com.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import com.neo4j.enterprise.edition.EnterpriseEditionModule;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.kernel.impl.factory.DatabaseInfo;

public class EnterpriseManagementServiceFactory
{
    private EnterpriseManagementServiceFactory()
    {
    }

    public static DatabaseManagementService createManagementService( Config config, ExternalDependencies dependencies )
    {
        GraphDatabaseSettings.Mode mode = config.get( GraphDatabaseSettings.mode );
        switch ( mode )
        {
        case CORE:
            return new CoreGraphDatabase( config, dependencies, newDiscoveryServiceFactory(), CoreEditionModule::new ).getManagementService();
        case READ_REPLICA:
            return new ReadReplicaGraphDatabase( config, dependencies, newDiscoveryServiceFactory(), ReadReplicaEditionModule::new ).getManagementService();
        case SINGLE:
            return new DatabaseManagementServiceFactory( DatabaseInfo.ENTERPRISE, EnterpriseEditionModule::new ).build( config, dependencies );
        default:
            throw new IllegalArgumentException( "Unknown mode: " + mode );
        }
    }

    private static AkkaDiscoveryServiceFactory newDiscoveryServiceFactory()
    {
        return new AkkaDiscoveryServiceFactory();
    }
}
