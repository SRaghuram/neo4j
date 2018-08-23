/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.discovery.SslDiscoveryServiceFactory;
import com.neo4j.causalclustering.handlers.SecurePipelineFactory;
import com.neo4j.dbms.database.MultiDatabaseManager;
import com.neo4j.kernel.availability.CompositeDatabaseAvailabilityGuard;
import com.neo4j.kernel.impl.transaction.stats.GlobalTransactionStats;

import java.time.Clock;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.discovery.DiscoveryServiceFactory;
import org.neo4j.causalclustering.handlers.DuplexPipelineWrapperFactory;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.readreplica.EnterpriseReadReplicaEditionModule;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.factory.module.EditionModule;
import org.neo4j.graphdb.factory.module.PlatformModule;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.impl.enterprise.EnterpriseEditionModule;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.impl.transaction.stats.TransactionCounters;
import org.neo4j.kernel.impl.util.Dependencies;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.Logger;
import org.neo4j.ssl.SslPolicy;

/**
 * This implementation of {@link EditionModule} creates the implementations of services
 * that are specific to the Enterprise Read Replica edition.
 */
public class CommercialReadReplicaEditionModule extends EnterpriseReadReplicaEditionModule
{
    private final GlobalTransactionStats transactionStats;

    CommercialReadReplicaEditionModule( final PlatformModule platformModule, final SslDiscoveryServiceFactory discoveryServiceFactory, MemberId myself )
    {
        super( platformModule, discoveryServiceFactory, myself );
        this.transactionStats = new GlobalTransactionStats();
        initGlobalGuard( platformModule.clock, platformModule.logging );
    }

    @Override
    protected void configureDiscoveryService( DiscoveryServiceFactory discoveryServiceFactory, Dependencies dependencies,
                                              Config config, LogProvider logProvider )
    {
        SslPolicyLoader sslPolicyFactory = dependencies.satisfyDependency( SslPolicyLoader.create( config, logProvider ) );
        SslPolicy clusterSslPolicy = sslPolicyFactory.getPolicy( config.get( CausalClusteringSettings.ssl_policy ) );

        if ( discoveryServiceFactory instanceof SslDiscoveryServiceFactory )
        {
            ((SslDiscoveryServiceFactory) discoveryServiceFactory).setSslPolicy( clusterSslPolicy );
        }
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
        GraphDatabaseFacade systemFacade = null; //databaseManager.createDatabase( MultiDatabaseManager.SYSTEM_DB_NAME );
        createConfiguredDatabases( databaseManager, systemFacade, config );
    }

    @Override
    protected DuplexPipelineWrapperFactory pipelineWrapperFactory()
    {
        return new SecurePipelineFactory();
    }

    private static void createConfiguredDatabases( DatabaseManager databaseManager, GraphDatabaseFacade systemFacade, Config config )
    {
        databaseManager.createDatabase( config.get( GraphDatabaseSettings.active_database ) );
    }

    @Override
    public DatabaseTransactionStats createTransactionMonitor()
    {
        return transactionStats.createDatabaseTransactionMonitor();
    }

    @Override
    public TransactionCounters globalTransactionCounter()
    {
        return transactionStats;
    }

    @Override
    public AvailabilityGuard getGlobalAvailabilityGuard( Clock clock, LogService logService )
    {
        initGlobalGuard( clock, logService );
        return globalAvailabilityGuard;
    }
    @Override
    public DatabaseAvailabilityGuard createDatabaseAvailabilityGuard( String databaseName, Clock clock, LogService logService )
    {
        return ((CompositeDatabaseAvailabilityGuard) getGlobalAvailabilityGuard( clock, logService )).createDatabaseAvailabilityGuard( databaseName );
    }

    @Override
    public void createSecurityModule( PlatformModule platformModule, Procedures procedures )
    {
        //TODO: change to commercial security module here when ready
        EnterpriseEditionModule.createEnterpriseSecurityModule( this, platformModule, procedures );
    }

    private void initGlobalGuard( Clock clock, LogService logService )
    {
        if ( globalAvailabilityGuard == null )
        {
            globalAvailabilityGuard = new CompositeDatabaseAvailabilityGuard( clock, logService );
        }
    }

}
