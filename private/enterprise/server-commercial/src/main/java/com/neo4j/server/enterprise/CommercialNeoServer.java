/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import java.io.File;

import com.neo4j.causalclustering.core.CommercialCoreGraphDatabase;
import com.neo4j.causalclustering.readreplica.CommercialReadReplicaGraphDatabase;
import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.ClusterSettings.Mode;
import org.neo4j.dbms.DatabaseManagementSystemSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Dependencies;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.LifecycleManagingDatabase.GraphFactory;
import org.neo4j.server.enterprise.OpenEnterpriseNeoServer;

import static org.neo4j.server.database.LifecycleManagingDatabase.lifecycleManagingDatabase;

public class CommercialNeoServer extends OpenEnterpriseNeoServer
{
    private static final GraphFactory CORE_FACTORY = ( config, dependencies ) ->
    {
        File storeDir = config.get( DatabaseManagementSystemSettings.database_path );
        return new CommercialCoreGraphDatabase( storeDir, config, dependencies );
    };

    private static final GraphFactory READ_REPLICA_FACTORY = ( config, dependencies ) ->
    {
        File storeDir = config.get( DatabaseManagementSystemSettings.database_path );
        return new CommercialReadReplicaGraphDatabase( storeDir, config, dependencies );
    };

    public CommercialNeoServer( Config config, Dependencies dependencies, LogProvider logProvider )
    {
        super( config, createDbFactory( config ), dependencies, logProvider );
    }

    protected static Database.Factory createDbFactory( Config config )
    {
        final Mode mode = config.get( ClusterSettings.mode );

        switch ( mode )
        {
        case CORE:
            return lifecycleManagingDatabase( CORE_FACTORY );
        case READ_REPLICA:
            return lifecycleManagingDatabase( READ_REPLICA_FACTORY );
        default:
            return OpenEnterpriseNeoServer.createDbFactory( config );
        }
    }
}
