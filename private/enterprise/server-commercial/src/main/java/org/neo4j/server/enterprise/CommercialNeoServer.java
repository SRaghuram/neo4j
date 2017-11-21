/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.enterprise;

import java.io.File;

import org.neo4j.causalclustering.core.CommercialCoreGraphDatabase;
import org.neo4j.causalclustering.readreplica.CommercialReadReplicaGraphDatabase;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Dependencies;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.LifecycleManagingDatabase.GraphFactory;

import static org.neo4j.server.database.LifecycleManagingDatabase.lifecycleManagingDatabase;

public class CommercialNeoServer extends EnterpriseNeoServer
{
    private static final GraphFactory CORE_FACTORY = ( config, dependencies ) ->
    {
        File storeDir = config.get( GraphDatabaseSettings.database_path );
        return new CommercialCoreGraphDatabase( storeDir, config, dependencies );
    };

    private static final GraphFactory READ_REPLICA_FACTORY = ( config, dependencies ) ->
    {
        File storeDir = config.get( GraphDatabaseSettings.database_path );
        return new CommercialReadReplicaGraphDatabase( storeDir, config, dependencies );
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
            return lifecycleManagingDatabase( CORE_FACTORY );
        case READ_REPLICA:
            return lifecycleManagingDatabase( READ_REPLICA_FACTORY );
        default:
            return EnterpriseNeoServer.createDbFactory( config );
        }
    }
}
