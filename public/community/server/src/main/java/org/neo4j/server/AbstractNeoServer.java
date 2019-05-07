/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.neo4j.common.DependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.connectors.HttpConnector.Encryption;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.internal.helpers.AdvertisedSocketAddress;
import org.neo4j.internal.helpers.ListenSocketAddress;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.api.security.AuthManager;
import org.neo4j.kernel.api.security.UserManagerSupplier;
import org.neo4j.kernel.internal.Version;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.internal.LogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.server.bind.ComponentsBinder;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.database.Database;
import org.neo4j.server.database.GraphFactory;
import org.neo4j.server.database.LifecycleManagingDatabase;
import org.neo4j.server.http.cypher.TransactionRegistry;
import org.neo4j.server.http.cypher.HttpTransactionManager;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.rest.repr.InputFormat;
import org.neo4j.server.rest.repr.InputFormatProvider;
import org.neo4j.server.rest.repr.OutputFormat;
import org.neo4j.server.rest.repr.OutputFormatProvider;
import org.neo4j.server.rest.repr.RepresentationFormatRepository;
import org.neo4j.server.web.RotatingRequestLog;
import org.neo4j.server.web.SimpleUriBuilder;
import org.neo4j.server.web.WebServer;
import org.neo4j.ssl.SslPolicy;
import org.neo4j.ssl.config.SslPolicyLoader;
import org.neo4j.time.Clocks;

import static org.neo4j.configuration.GraphDatabaseSettings.db_timezone;
import static org.neo4j.server.configuration.ServerSettings.http_log_path;
import static org.neo4j.server.configuration.ServerSettings.http_logging_enabled;
import static org.neo4j.server.configuration.ServerSettings.http_logging_rotation_keep_number;
import static org.neo4j.server.configuration.ServerSettings.http_logging_rotation_size;
import static org.neo4j.server.exception.ServerStartupErrors.translateToServerStartupError;

public abstract class AbstractNeoServer implements NeoServer
{
    private static final long MINIMUM_TIMEOUT = 1000L;
    /**
     * We add a second to the timeout if the user configures a 1-second timeout.
     * <p>
     * This ensures the expiry time displayed to the user is always at least 1 second, even after it is rounded down.
     */
    private static final long ROUNDING_SECOND = 1000L;

    private static final Pattern[] DEFAULT_URI_WHITELIST = new Pattern[]{
            Pattern.compile( "/browser.*" ),
            Pattern.compile( "/" )
    };
    public static final String NEO4J_IS_STARTING_MESSAGE = "======== Neo4j " + Version.getNeo4jVersion() + " ========";

    protected final LogProvider userLogProvider;
    private final Log log;

    private final List<ServerModule> serverModules = new ArrayList<>();
    private final SimpleUriBuilder uriBuilder = new SimpleUriBuilder();
    private final Config config;
    private final LifeSupport life = new LifeSupport();
    private final ListenSocketAddress httpListenAddress;
    private final ListenSocketAddress httpsListenAddress;
    private AdvertisedSocketAddress httpAdvertisedAddress;
    private AdvertisedSocketAddress httpsAdvertisedAddress;

    protected final Database database;
    private DependencyResolver dependencyResolver;
    protected WebServer webServer;
    protected Supplier<AuthManager> authManagerSupplier;
    protected Supplier<UserManagerSupplier> userManagerSupplier;
    protected Supplier<SslPolicyLoader> sslPolicyFactorySupplier;
    private HttpTransactionManager httpTransactionManager;

    private ConnectorPortRegister connectorPortRegister;
    private HttpConnector httpConnector;
    private HttpConnector httpsConnector;
    private RotatingRequestLog requestLog;

    protected abstract Iterable<ServerModule> createServerModules();

    protected abstract WebServer createWebServer();

    public AbstractNeoServer( Config config, GraphFactory graphFactory, ExternalDependencies dependencies )
    {
        this.config = config;
        this.userLogProvider = dependencies.userLogProvider();
        this.log = userLogProvider.getLog( getClass() );
        log.info( NEO4J_IS_STARTING_MESSAGE );

        verifyConnectorsConfiguration( config );

        httpConnector = findConnector( config, Encryption.NONE );
        httpListenAddress = listenAddressFor( config, httpConnector );
        httpAdvertisedAddress = advertisedAddressFor( config, httpConnector );

        httpsConnector = findConnector( config, Encryption.TLS );
        httpsListenAddress = listenAddressFor( config, httpsConnector );
        httpsAdvertisedAddress = advertisedAddressFor( config, httpsConnector );

        database = new LifecycleManagingDatabase( config, graphFactory, dependencies );
        life.add( database );
        life.add( new ServerDependenciesLifeCycleAdapter() );
        life.add( new ServerComponentsLifecycleAdapter() );
    }

    @Override
    public void start() throws ServerStartupException
    {
        try
        {
            life.start();
        }
        catch ( Throwable t )
        {
            // If the database has been started, attempt to cleanly shut it down to avoid unclean shutdowns.
            life.shutdown();

            throw translateToServerStartupError( t );
        }
    }

    public DependencyResolver getDependencyResolver()
    {
        return dependencyResolver;
    }

    private HttpTransactionManager createHttpTransactionManager( DatabaseManager databaseManager )
    {
        JobScheduler jobScheduler = resolveDependency( JobScheduler.class );
        Clock clock = Clocks.systemClock();
        Duration transactionTimeout = getTransactionTimeout();
        return new HttpTransactionManager( databaseManager, config, jobScheduler, clock, transactionTimeout, userLogProvider );
    }

    /**
     * We are going to ensure the minimum timeout is 2 seconds. The timeout value is communicated to the user in
     * seconds rounded down, meaning if a user set a 1 second timeout, he would be told there was less than 1 second
     * remaining before he would need to renew the timeout.
     */
    private Duration getTransactionTimeout()
    {
        final long timeout = config.get( ServerSettings.transaction_idle_timeout ).toMillis();
        return Duration.ofMillis( Math.max( timeout, MINIMUM_TIMEOUT + ROUNDING_SECOND ) );
    }

    /**
     * Use this method to register server modules from subclasses
     */
    protected final void registerModule( ServerModule module )
    {
        serverModules.add( module );
    }

    private void startModules()
    {
        for ( ServerModule module : serverModules )
        {
            module.start();
        }
    }

    private void stopModules()
    {
        final List<Exception> errors = new ArrayList<>();
        for ( final ServerModule module : serverModules )
        {
            try
            {
                module.stop();
            }
            catch ( Exception e )
            {
                errors.add( e );
            }
        }
        if ( !errors.isEmpty() )
        {
            final RuntimeException e = new RuntimeException();
            errors.forEach( e::addSuppressed );
            throw e;
        }
    }

    private void clearModules()
    {
        serverModules.clear();
    }

    @Override
    public Config getConfig()
    {
        return config;
    }

    protected void configureWebServer()
    {
        webServer.setHttpAddress( httpListenAddress );
        webServer.setHttpsAddress( httpsListenAddress );
        webServer.setMaxThreads( config.get( ServerSettings.webserver_max_threads ) );
        webServer.setWadlEnabled( config.get( ServerSettings.wadl_enabled ) );
        webServer.setComponentsBinder( createComponentsBinder() );

        String sslPolicyName = config.get( ServerSettings.ssl_policy );
        if ( sslPolicyName != null )
        {
            SslPolicy sslPolicy = sslPolicyFactorySupplier.get().getPolicy( sslPolicyName );
            webServer.setSslPolicy( sslPolicy );
        }
    }

    protected void startWebServer() throws Exception
    {
        try
        {
            setUpHttpLogging();
            webServer.start();
            registerHttpAddressAfterStartup();
            registerHttpsAddressAfterStartup();
            log.info( "Remote interface available at %s", baseUri() );
        }
        catch ( Exception e )
        {
            ListenSocketAddress address = httpListenAddress != null ? httpListenAddress : httpsListenAddress;
            log.error( "Failed to start Neo4j on %s: %s", address, e.getMessage() );
            throw e;
        }
    }

    private void registerHttpAddressAfterStartup()
    {
        if ( httpConnector != null )
        {
            InetSocketAddress localHttpAddress = webServer.getLocalHttpAddress();
            connectorPortRegister.register( httpConnector.key(), localHttpAddress );
            if ( httpAdvertisedAddress.getPort() == 0 )
            {
                httpAdvertisedAddress = new AdvertisedSocketAddress( localHttpAddress.getHostString(), localHttpAddress.getPort() );
            }
        }
    }

    private void registerHttpsAddressAfterStartup()
    {
        if ( httpsConnector != null )
        {
            InetSocketAddress localHttpsAddress = webServer.getLocalHttpsAddress();
            connectorPortRegister.register( httpsConnector.key(), localHttpsAddress );
            if ( httpsAdvertisedAddress.getPort() == 0 )
            {
                httpsAdvertisedAddress = new AdvertisedSocketAddress( localHttpsAddress.getHostString(), localHttpsAddress.getPort() );
            }
        }
    }

    private void setUpHttpLogging() throws IOException
    {
        if ( !getConfig().get( http_logging_enabled ) )
        {
            return;
        }

        requestLog = new RotatingRequestLog(
                dependencyResolver.resolveDependency( FileSystemAbstraction.class ),
                dependencyResolver.resolveDependency( JobScheduler.class ),
                config.get( db_timezone ).getZoneId(),
                config.get( http_log_path ).toString(),
                config.get( http_logging_rotation_size ),
                config.get( http_logging_rotation_keep_number ) );
        webServer.setRequestLog( requestLog );
    }

    protected Pattern[] getUriWhitelist()
    {
        return DEFAULT_URI_WHITELIST;
    }

    @Override
    public void stop()
    {
        life.stop();
    }

    private void stopWebServer() throws Exception
    {
        if ( webServer != null )
        {
            webServer.stop();
        }
        if ( requestLog != null )
        {
            requestLog.stop();
        }
    }

    @Override
    public Database getDatabase()
    {
        return database;
    }

    @Override
    public TransactionRegistry getTransactionRegistry()
    {
        return httpTransactionManager.getTransactionHandleRegistry();
    }

    @Override
    public URI baseUri()
    {
        return httpAdvertisedAddress != null
               ? uriBuilder.buildURI( httpAdvertisedAddress, false )
               : uriBuilder.buildURI( httpsAdvertisedAddress, true );
    }

    public Optional<URI> httpsUri()
    {
        return Optional.ofNullable( httpsAdvertisedAddress )
                .map( address -> uriBuilder.buildURI( address, true ) );
    }

    public WebServer getWebServer()
    {
        return webServer;
    }

    private ComponentsBinder createComponentsBinder()
    {
        Database database = getDatabase();

        ComponentsBinder binder = new ComponentsBinder();

        binder.addSingletonBinding( database, Database.class );
        binder.addSingletonBinding( database.getGraph(), GraphDatabaseService.class );
        binder.addSingletonBinding( this, NeoServer.class );
        binder.addSingletonBinding( getConfig(), Config.class );
        binder.addSingletonBinding( getWebServer(), WebServer.class );
        binder.addSingletonBinding( new RepresentationFormatRepository( this ), RepresentationFormatRepository.class );
        binder.addLazyBinding( InputFormatProvider.class, InputFormat.class );
        binder.addLazyBinding( OutputFormatProvider.class, OutputFormat.class );
        binder.addSingletonBinding( httpTransactionManager, HttpTransactionManager.class );
        binder.addLazyBinding( authManagerSupplier, AuthManager.class );
        binder.addLazyBinding( userManagerSupplier, UserManagerSupplier.class );
        binder.addSingletonBinding( userLogProvider, LogProvider.class );
        binder.addSingletonBinding( userLogProvider.getLog( NeoServer.class ), Log.class );

        return binder;
    }

    protected <T> T resolveDependency( Class<T> type )
    {
        return dependencyResolver.resolveDependency( type );
    }

    private static void verifyConnectorsConfiguration( Config config )
    {
        HttpConnector httpConnector = findConnector( config, Encryption.NONE );
        HttpConnector httpsConnector = findConnector( config, Encryption.TLS );

        if ( httpConnector == null && httpsConnector == null )
        {
            throw new IllegalArgumentException( "Either HTTP or HTTPS connector must be configured to run the server" );
        }
    }

    private static HttpConnector findConnector( Config config, Encryption encryption )
    {
        return config.enabledHttpConnectors()
                .stream()
                .filter( connector -> connector.encryptionLevel() == encryption )
                .findFirst()
                .orElse( null );
    }

    private static ListenSocketAddress listenAddressFor( Config config, HttpConnector connector )
    {
        return connector == null ? null : config.get( connector.listen_address );
    }

    private static AdvertisedSocketAddress advertisedAddressFor( Config config, HttpConnector connector )
    {
        return connector == null ? null : config.get( connector.advertised_address );
    }

    private class ServerDependenciesLifeCycleAdapter extends LifecycleAdapter
    {
        @Override
        public void start()
        {
            dependencyResolver = database.getGraph().getDependencyResolver();

            authManagerSupplier = dependencyResolver.provideDependency( AuthManager.class );
            userManagerSupplier = dependencyResolver.provideDependency( UserManagerSupplier.class );
            sslPolicyFactorySupplier = dependencyResolver.provideDependency( SslPolicyLoader.class );
            webServer = createWebServer();

            for ( ServerModule moduleClass : createServerModules() )
            {
                registerModule( moduleClass );
            }
        }
    }

    private class ServerComponentsLifecycleAdapter extends LifecycleAdapter
    {
        @Override
        public void start() throws Exception
        {
            LogService logService = resolveDependency( LogService.class );
            Log serverLog = logService.getInternalLog( ServerComponentsLifecycleAdapter.class );
            serverLog.info( "Starting web server" );
            connectorPortRegister = dependencyResolver.resolveDependency( ConnectorPortRegister.class );

            DatabaseManager databaseManager = database.getGraph().getDependencyResolver().resolveDependency( DatabaseManager.class );
            httpTransactionManager = createHttpTransactionManager(databaseManager);

            configureWebServer();

            startModules();

            startWebServer();

            serverLog.info( "Web server started." );
        }

        @Override
        public void stop() throws Exception
        {
            stopWebServer();
            stopModules();
            clearModules();
        }
    }
}
