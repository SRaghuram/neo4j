/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.api;

import com.neo4j.causalclustering.core.CoreEditionModule;
import com.neo4j.causalclustering.discovery.akka.AkkaDiscoveryServiceFactory;
import com.neo4j.causalclustering.readreplica.ReadReplicaEditionModule;

import java.io.File;
import java.util.function.Function;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.kernel.impl.factory.DbmsInfo;

import static java.lang.Boolean.FALSE;

@PublicApi
public class ClusterDatabaseManagementServiceBuilder extends EnterpriseDatabaseManagementServiceBuilder
{
    public ClusterDatabaseManagementServiceBuilder( File homeDirectory )
    {
        super( homeDirectory );
    }

    @Override
    protected ClusterDatabaseManagementService newDatabaseManagementService( Config config, ExternalDependencies dependencies )
    {
        config.set( GraphDatabaseSettings.ephemeral_lucene, FALSE );
        return new ClusterDatabaseManagementServiceFactory( getDbmsInfo( config ), getEditionFactory( config ) )
                .build( augmentConfig( config ), dependencies );
    }

    @Override
    public ClusterDatabaseManagementService build()
    {
        return (ClusterDatabaseManagementService) super.build();
    }

    @Override
    protected DbmsInfo getDbmsInfo( Config config )
    {
        GraphDatabaseSettings.Mode mode = config.get( GraphDatabaseSettings.mode );
        switch ( mode )
        {
        case CORE:
            return DbmsInfo.CORE;
        case READ_REPLICA:
            return DbmsInfo.READ_REPLICA;
        default:
            throw new IllegalArgumentException( "Unsupported mode: " + mode );
        }
    }

    @Override
    protected Function<GlobalModule,AbstractEditionModule> getEditionFactory( Config config )
    {
        GraphDatabaseSettings.Mode mode = config.get( GraphDatabaseSettings.mode );
        switch ( mode )
        {
        case CORE:
            return globalModule -> new CoreEditionModule( globalModule, new AkkaDiscoveryServiceFactory() );
        case READ_REPLICA:
            return globalModule -> new ReadReplicaEditionModule( globalModule, new AkkaDiscoveryServiceFactory() );
        default:
            throw new IllegalArgumentException( "Unsupported mode: " + mode );
        }
    }
}
