/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.causalclustering.core.CommercialCoreGraphDatabase;
import com.neo4j.causalclustering.readreplica.CommercialReadReplicaGraphDatabase;
import com.neo4j.commercial.edition.CommercialGraphDatabase;

import java.io.File;

import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory.Dependencies;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.LifecycleManagingDatabase.GraphFactory;
import org.neo4j.server.enterprise.OpenEnterpriseNeoServer;

import static org.neo4j.server.database.LifecycleManagingDatabase.lifecycleManagingDatabase;

public class CommercialNeoServer extends OpenEnterpriseNeoServer
{
    private static final GraphFactory COMMERCIAL_CORE_FACTORY = ( config, dependencies ) ->
    {
        File storeDir = config.get( GraphDatabaseSettings.databases_root_path );
        return new CommercialCoreGraphDatabase( storeDir, config, dependencies );
    };

    private static final GraphFactory COMMERCIAL_READ_REPLICA_FACTORY = ( config, dependencies ) ->
    {
        File storeDir = config.get( GraphDatabaseSettings.databases_root_path );
        return new CommercialReadReplicaGraphDatabase( storeDir, config, dependencies );
    };

    private static final GraphFactory COMMERCIAL_ENTERPRISE_FACTORY = ( config, dependencies ) ->
    {
        File storeDir = config.get( GraphDatabaseSettings.database_path );
        return new CommercialGraphDatabase( storeDir, config, dependencies );
    };

    public CommercialNeoServer( Config config, Dependencies dependencies, LogProvider logProvider )
    {
        super( config, createDbFactory( config ), dependencies, logProvider );
    }

    protected static Database.Factory createDbFactory( Config config )
    {
        final EnterpriseEditionSettings.Mode mode = config.get( EnterpriseEditionSettings.mode );

        switch ( mode )
        {
        case CORE:
            return lifecycleManagingDatabase( COMMERCIAL_CORE_FACTORY );
        case READ_REPLICA:
            return lifecycleManagingDatabase( COMMERCIAL_READ_REPLICA_FACTORY );
        case SINGLE:
            return lifecycleManagingDatabase( COMMERCIAL_ENTERPRISE_FACTORY );
        default:
            return OpenEnterpriseNeoServer.createDbFactory( config );
        }
    }
}
