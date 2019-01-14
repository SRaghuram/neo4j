/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import com.neo4j.server.database.CommercialGraphFactory;
import com.neo4j.server.enterprise.modules.EnterpriseAuthorizationModule;
import com.neo4j.server.enterprise.modules.JMXManagementModule;
import com.neo4j.server.rest.DatabaseRoleInfoServerModule;
import com.neo4j.server.rest.EnterpriseDiscoverableURIs;
import org.eclipse.jetty.util.thread.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.exceptions.UnsatisfiedDependencyException;
import org.neo4j.graphdb.facade.GraphDatabaseFacadeFactory.Dependencies;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ConnectorPortRegister;
import org.neo4j.logging.Log;
import org.neo4j.metrics.source.server.ServerThreadView;
import org.neo4j.metrics.source.server.ServerThreadViewSetter;
import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.database.GraphFactory;
import org.neo4j.server.modules.AuthorizationModule;
import org.neo4j.server.modules.DBMSModule;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.rest.discovery.DiscoverableURIs;
import org.neo4j.server.web.Jetty9WebServer;
import org.neo4j.server.web.WebServer;

import static org.neo4j.server.configuration.ServerSettings.jmx_module_enabled;

public class CommercialNeoServer extends CommunityNeoServer
{
    public CommercialNeoServer( Config config, Dependencies dependencies )
    {
        super( config, new CommercialGraphFactory(), dependencies );
    }

    public CommercialNeoServer( Config config, GraphFactory graphFactory, Dependencies dependencies )
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
                        database.getGraph().getDependencyResolver().resolveDependency( ServerThreadViewSetter.class );
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
        Supplier<DiscoverableURIs> discoverableURIs  = () -> EnterpriseDiscoverableURIs.enterpriseDiscoverableURIs(
                        getConfig(), getDependencyResolver().resolveDependency( ConnectorPortRegister.class ) );
        return new DBMSModule( webServer, getConfig(), discoverableURIs );
    }

    @SuppressWarnings( "unchecked" )
    @Override
    protected Iterable<ServerModule> createServerModules()
    {
        List<ServerModule> modules = new ArrayList<>();
        modules.add( new DatabaseRoleInfoServerModule( webServer, getConfig(), userLogProvider ) );
        if ( getConfig().get( jmx_module_enabled ) )
        {
            modules.add( new JMXManagementModule( this ) );
        }
        super.createServerModules().forEach( modules::add );
        return modules;
    }

    @Override
    protected Pattern[] getUriWhitelist()
    {
        final List<Pattern> uriWhitelist = new ArrayList<>( Arrays.asList( super.getUriWhitelist() ) );

        if ( !getConfig().get( CausalClusteringSettings.status_auth_enabled ) )
        {
            uriWhitelist.add( Pattern.compile( "/db/manage/server/core.*" ) );
            uriWhitelist.add( Pattern.compile( "/db/manage/server/read-replica.*" ) );
            uriWhitelist.add( Pattern.compile( "/db/manage/server/causalclustering.*" ) );
        }

        return uriWhitelist.toArray( new Pattern[uriWhitelist.size()] );
    }
}
