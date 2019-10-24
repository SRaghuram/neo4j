/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.causalclustering.core.CausalClusteringSettings;
import com.neo4j.metrics.source.server.ServerThreadView;
import com.neo4j.metrics.source.server.ServerThreadViewSetter;
import com.neo4j.server.database.EnterpriseGraphFactory;
import com.neo4j.server.enterprise.modules.EnterpriseAuthorizationModule;
import com.neo4j.server.rest.DatabaseRoleInfoServerModule;
import com.neo4j.server.rest.LegacyManagementModule;
import com.neo4j.server.rest.causalclustering.CausalClusteringService;
import com.neo4j.server.rest.causalclustering.LegacyCausalClusteringRedirectService;
import org.eclipse.jetty.util.thread.ThreadPool;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.exceptions.UnsatisfiedDependencyException;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.logging.Log;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.database.GraphFactory;
import org.neo4j.server.modules.AuthorizationModule;
import org.neo4j.server.modules.DBMSModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.rest.discovery.DiscoverableURIs;
import org.neo4j.server.web.Jetty9WebServer;
import org.neo4j.server.web.WebServer;

import static com.neo4j.server.rest.EnterpriseDiscoverableURIs.enterpriseDiscoverableURIs;

public class EnterpriseNeoServer extends CommunityNeoServer
{
    public EnterpriseNeoServer( Config config, ExternalDependencies dependencies )
    {
        super( config, new EnterpriseGraphFactory(), dependencies );
    }

    public EnterpriseNeoServer( Config config, GraphFactory graphFactory, ExternalDependencies dependencies )
    {
        super( config, graphFactory, dependencies );
    }

    @Override
    protected WebServer createWebServer()
    {
        Jetty9WebServer webServer = (Jetty9WebServer) super.createWebServer();
        webServer.setJettyCreatedCallback( jetty ->
        {
            ThreadPool threadPool = jetty.getThreadPool();
            assert threadPool != null;
            try
            {
                ServerThreadViewSetter setter =
                        databaseService.getSystemDatabase().getDependencyResolver().resolveDependency( ServerThreadViewSetter.class );
                setter.set( new ServerThreadView()
                {
                    @Override
                    public int allThreads()
                    {
                        return threadPool.getThreads();
                    }

                    @Override
                    public int idleThreads()
                    {
                        return threadPool.getIdleThreads();
                    }
                } );
            }
            catch ( UnsatisfiedDependencyException ex )
            {
                Log log = userLogProvider.getLog( getClass() );
                log.warn( "Metrics dependencies not found.", ex );
            }
        } );
        return webServer;
    }

    @Override
    protected AuthorizationModule createAuthorizationModule()
    {
        return new EnterpriseAuthorizationModule( webServer, authManagerSupplier, userLogProvider, getConfig(),
                getUriWhitelist() );
    }

    @Override
    protected DBMSModule createDBMSModule()
    {
        // ConnectorPortRegister isn't available until runtime, so defer loading until then
        Supplier<DiscoverableURIs> discoverableURIs  = () -> enterpriseDiscoverableURIs(
                getConfig(), getSystemDatabaseDependencyResolver().resolveDependency( ConnectorPortRegister.class ) );
        return new DBMSModule( webServer, getConfig(), discoverableURIs, userLogProvider );
    }

    @Override
    protected Iterable<ServerModule> createServerModules()
    {
        List<ServerModule> modules = new ArrayList<>();
        modules.add( new DatabaseRoleInfoServerModule( webServer, getConfig() ) );
        modules.add( new LegacyManagementModule( webServer, getConfig() ) );
        super.createServerModules().forEach( modules::add );
        return modules;
    }

    @Override
    protected List<Pattern> getUriWhitelist()
    {
        var result = new ArrayList<>( super.getUriWhitelist() );
        if ( !getConfig().get( CausalClusteringSettings.status_auth_enabled ) )
        {
            result.add( CausalClusteringService.databaseClusterUriPattern( getConfig() ) );
            result.add( LegacyCausalClusteringRedirectService.databaseLegacyClusterUriPattern( getConfig() ) );
        }
        return result;
    }
}
