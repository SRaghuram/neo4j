/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.discovery.SslHazelcastDiscoveryServiceFactory;
import com.neo4j.causalclustering.handlers.SecurePipelineFactory;
import com.neo4j.dbms.database.MultiDatabaseManager;

import java.io.File;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.EnterpriseCoreEditionModule;
import org.neo4j.causalclustering.core.IdentityModule;
import org.neo4j.causalclustering.core.state.ClusterStateDirectory;
import org.neo4j.causalclustering.core.state.ClusteringModule;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.handlers.DuplexPipelineWrapperFactory;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.factory.module.EditionModule;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.impl.enterprise.EnterpriseEditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.Logger;
import org.neo4j.ssl.SslPolicy;

/**
 * This implementation of {@link EditionModule} creates the implementations of services
 * that are specific to the Enterprise Core edition that provides a core cluster.
 */
public class CommercialCoreEditionModule extends EnterpriseCoreEditionModule
{
    CommercialCoreEditionModule( final PlatformModule platformModule, final DiscoveryServiceFactory discoveryServiceFactory )
    {
        super( platformModule, discoveryServiceFactory );
    }

    @Override
    public void setupSecurityModule( PlatformModule platformModule, Procedures procedures )
    {
        EnterpriseEditionModule.setupEnterpriseSecurityModule( this, platformModule, procedures );
    }

    protected ClusteringModule getClusteringModule( PlatformModule platformModule, DiscoveryServiceFactory discoveryServiceFactory,
            ClusterStateDirectory clusterStateDirectory, IdentityModule identityModule, Dependencies dependencies, File databaseDirectory )
    {
        SslPolicyLoader sslPolicyFactory = dependencies.satisfyDependency( SslPolicyLoader.create( config, logProvider ) );
        SslPolicy clusterSslPolicy = sslPolicyFactory.getPolicy( config.get( CausalClusteringSettings.ssl_policy ) );

        if ( discoveryServiceFactory instanceof SslHazelcastDiscoveryServiceFactory )
        {
            ((SslHazelcastDiscoveryServiceFactory) discoveryServiceFactory).setSslPolicy( clusterSslPolicy );
        }

        return new ClusteringModule( discoveryServiceFactory, identityModule.myself(), platformModule, clusterStateDirectory.get(), databaseDirectory );
    }

    @Override
    public DatabaseManager createDatabaseManager( GraphDatabaseFacade graphDatabaseFacade, PlatformModule platform, EditionModule edition,
            Procedures procedures, Logger msgLog )
    {
        return new MultiDatabaseManager( platform, edition, procedures, msgLog, graphDatabaseFacade );
    }

    @Override
    public void createDatabases( DatabaseManager databaseManager, Config config )
    {
        super.createDatabases( databaseManager, config );
        databaseManager.createDatabase( MultiDatabaseManager.SYSTEM_DB_NAME );
    }

    @Override
    protected DuplexPipelineWrapperFactory pipelineWrapperFactory()
    {
        return new SecurePipelineFactory();
    }
}
