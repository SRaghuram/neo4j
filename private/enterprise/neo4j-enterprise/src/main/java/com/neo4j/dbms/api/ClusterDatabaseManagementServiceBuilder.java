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
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Function;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.event.DatabaseEventListener;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.AbstractEditionModule;
import org.neo4j.graphdb.security.URLAccessRule;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.logging.LogProvider;
import org.neo4j.monitoring.Monitors;

import static java.lang.Boolean.FALSE;

@PublicApi
public class ClusterDatabaseManagementServiceBuilder extends EnterpriseDatabaseManagementServiceBuilder
{
    /**
     * @deprecated Use {@link #ClusterDatabaseManagementServiceBuilder(Path)}.
     */
    @Deprecated( forRemoval = true )
    public ClusterDatabaseManagementServiceBuilder( File homeDirectory )
    {
        this( homeDirectory.toPath() );
    }

    public ClusterDatabaseManagementServiceBuilder( Path homeDirectory )
    {
        super( homeDirectory );
    }

    @Override
    protected ClusterDatabaseManagementService newDatabaseManagementService( Config config, ExternalDependencies dependencies )
    {
        config.set( GraphDatabaseInternalSettings.ephemeral_lucene, FALSE );
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

    @Override
    public ClusterDatabaseManagementServiceBuilder addDatabaseListener( DatabaseEventListener databaseEventListener )
    {
        super.addDatabaseListener( databaseEventListener );
        return this;
    }

    @Override
    public ClusterDatabaseManagementServiceBuilder addURLAccessRule( String protocol, URLAccessRule rule )
    {
        super.addURLAccessRule( protocol, rule );
        return this;
    }

    @Override
    public ClusterDatabaseManagementServiceBuilder setUserLogProvider( LogProvider userLogProvider )
    {
        super.setUserLogProvider( userLogProvider );
        return this;
    }

    @Override
    public ClusterDatabaseManagementServiceBuilder setMonitors( Monitors monitors )
    {
        super.setMonitors( monitors );
        return this;
    }

    @Override
    public ClusterDatabaseManagementServiceBuilder setExternalDependencies( DependencyResolver dependencies )
    {
        super.setExternalDependencies( dependencies );
        return this;
    }

    @Override
    public <T> ClusterDatabaseManagementServiceBuilder setConfig( Setting<T> setting, T value )
    {
        super.setConfig( setting, value );
        return this;
    }

    @Override
    public ClusterDatabaseManagementServiceBuilder setConfig( Map<Setting<?>,Object> config )
    {
        super.setConfig( config );
        return this;
    }

    @Override
    public ClusterDatabaseManagementServiceBuilder setConfigRaw( Map<String, String> raw )
    {
        super.setConfigRaw( raw );
        return this;
    }

    /**
     * @deprecated Use {@link #loadPropertiesFromFile(Path)} instead.
     */
    @Override
    @Deprecated( forRemoval = true )
    public ClusterDatabaseManagementServiceBuilder loadPropertiesFromFile( String fileName )
    {
        loadPropertiesFromFile( Path.of( fileName ) );
        return this;
    }

    @Override
    public ClusterDatabaseManagementServiceBuilder loadPropertiesFromFile( Path path )
    {
        super.loadPropertiesFromFile( path );
        return this;
    }
}
