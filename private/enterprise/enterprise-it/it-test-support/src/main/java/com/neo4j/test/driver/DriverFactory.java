/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.driver;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.neo4j.driver.AuthToken;
import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.net.ServerAddress;
import org.neo4j.driver.net.ServerAddressResolver;
import org.neo4j.logging.Level;
import org.neo4j.logging.Log;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.test.rule.TestDirectory;

public class DriverFactory extends CloseableFactory
{
    static final InstanceConfig defaultConfig = new InstanceConfig( AuthTokens.none(), Level.DEBUG, null, false, null );

    private static final URI BOOTSTRAP_URI = URI.create( "neo4j://ignore.com" );
    static final String LOGS_DIR = "driver-logs";

    private final AtomicLong driverCounter = new AtomicLong();
    private final Map<Driver,Path> logFiles = new IdentityHashMap<>();
    private final TestDirectory testDirectory;

    DriverFactory( TestDirectory testDirectory )
    {
        this.testDirectory = testDirectory;
    }

    private Log createNewLogger( Path logFile, Level logLevel ) throws IOException
    {
        var fs = testDirectory.getFileSystem();
        var outputStream = fs.openAsOutputStream( logFile, false );
        addCloseable( outputStream );
        Log4jLogProvider log4jLogProvider = new Log4jLogProvider( outputStream, logLevel );
        addCloseable( log4jLogProvider );
        return log4jLogProvider.getLog( getClass() );
    }

    private Path createNewLogFile() throws IOException
    {
        var logDir = getLogDir();
        testDirectory.getFileSystem().mkdir( logDir );
        return logDir.resolve( "driver-" + driverCounter.incrementAndGet() + ".log" );
    }

    private Path getLogDir()
    {
        return testDirectory.homePath().resolve( LOGS_DIR );
    }

    private Logging createLogger( Log log, Level logLevel )
    {
        return name -> new TranslatedLogger( log, logLevel );
    }

    private Config.ConfigBuilder createDefaultConfigBuilder( Path logFile, Level logLevel ) throws IOException
    {
        return Config.builder().withoutEncryption().withLogging( createLogger( createNewLogger( logFile, logLevel ), logLevel ) );
    }

    public static InstanceConfig instanceConfig()
    {
        return new InstanceConfig( defaultConfig.authToken, defaultConfig.logLevel, defaultConfig.resolver, defaultConfig.encryptionEnabled,
                                   defaultConfig.additionalConfig );
    }

    public Driver graphDatabaseDriver( URI uri ) throws IOException
    {
        return graphDatabaseDriver( uri, defaultConfig );
    }

    public Driver graphDatabaseDriver( URI uri, InstanceConfig config ) throws IOException
    {
        return getDriver( uri, config );
    }

    public Driver graphDatabaseDriver( String uri ) throws IOException
    {
        return graphDatabaseDriver( URI.create( uri ), defaultConfig );
    }

    public Driver graphDatabaseDriver( String uri, InstanceConfig config ) throws IOException
    {
        return getDriver( URI.create( uri ), config );
    }

    public Driver graphDatabaseDriver( Collection<URI> boltURIs ) throws IOException
    {
        return graphDatabaseDriver( boltURIs, defaultConfig );
    }

    public Driver graphDatabaseDriver( Collection<URI> boltURIs, InstanceConfig config ) throws IOException
    {
        config = config.withResolver( address -> address.host().equals( BOOTSTRAP_URI.getHost() )
                                                 ? boltURIs.stream().map( uri -> ServerAddress.of( uri.getHost(), uri.getPort() ) )
                                                           .collect( Collectors.toSet() )
                                                 : Set.of( address ) );
        return graphDatabaseDriver( BOOTSTRAP_URI, config );
    }

    public Driver graphDatabaseDriver( ServerAddressResolver resolver ) throws IOException
    {
        return graphDatabaseDriver( resolver, defaultConfig );
    }

    public Driver graphDatabaseDriver( ServerAddressResolver resolver, InstanceConfig config ) throws IOException
    {
        config = config.withResolver( resolver );
        return getDriver( BOOTSTRAP_URI, config );
    }

    private Driver getDriver( URI uri, InstanceConfig instanceConfig ) throws IOException
    {
        Path logFile = createNewLogFile();
        var configBuilder = createDefaultConfigBuilder( logFile, instanceConfig.logLevel );
        configBuilder = instanceConfig.fill( configBuilder );
        Driver driver = GraphDatabase.driver( uri, instanceConfig.authToken, configBuilder.build() );
        linkLogFileToDriver( driver, logFile );
        return addCloseable( driver );
    }

    private void linkLogFileToDriver( Driver driver, Path logFile )
    {
        synchronized ( logFiles )
        {
            logFiles.put( driver, logFile );
        }
    }

    public Path getLogFile( Driver driver )
    {
        return logFiles.get( driver );
    }

    private static final class TranslatedLogger implements Logger
    {

        private final Log log;
        private final Level level;

        private TranslatedLogger( Log log, Level level )
        {
            this.log = log;
            this.level = level;
        }

        @Override
        public void error( String message, Throwable cause )
        {
            log.error( message, cause );
        }

        @Override
        public void info( String message, Object... params )
        {
            log.info( message, params );
        }

        @Override
        public void warn( String message, Object... params )
        {
            log.warn( message, params );
        }

        @Override
        public void warn( String message, Throwable cause )
        {
            log.warn( message, cause );
        }

        @Override
        public void debug( String message, Object... params )
        {
            log.debug( message, params );
        }

        @Override
        public void trace( String message, Object... params )
        {
            log.debug( message, params );
        }

        @Override
        public boolean isTraceEnabled()
        {
            return false;
        }

        @Override
        public boolean isDebugEnabled()
        {
            return level == Level.DEBUG;
        }
    }

    public static class InstanceConfig
    {
        private final AuthToken authToken;
        private final Level logLevel;
        private final ServerAddressResolver resolver;
        private final UnaryOperator<Config.ConfigBuilder> additionalConfig;
        private final boolean encryptionEnabled;

        private InstanceConfig( AuthToken authToken, Level logLevel, ServerAddressResolver resolver,
                                boolean encryptionEnabled, UnaryOperator<Config.ConfigBuilder> additionalConfig )
        {

            this.authToken = authToken;
            this.logLevel = logLevel;
            this.resolver = resolver;
            this.encryptionEnabled = encryptionEnabled;
            this.additionalConfig = additionalConfig;
        }

        public InstanceConfig withAuthToken( AuthToken authToken )
        {
            return new InstanceConfig( authToken, logLevel, resolver, encryptionEnabled, additionalConfig );
        }

        public InstanceConfig withLogLevel( Level logLevel )
        {
            return new InstanceConfig( authToken, logLevel, resolver, encryptionEnabled, additionalConfig );
        }

        /*
        This is private because we expect people to supply the resolver in the call to the DriverFactory but the Drivers consider Resolver to be part of the
        driver config so we set it inside the DriverFactory
         */
        private InstanceConfig withResolver( ServerAddressResolver resolver )
        {
            if ( this.resolver != null )
            {
                throw new IllegalStateException( "cannot set resolver multiple times" );
            }
            return new InstanceConfig( authToken, logLevel, resolver, encryptionEnabled, additionalConfig );
        }

        public InstanceConfig withEncryptionEnabled( boolean encryptionEnabled )
        {
            return new InstanceConfig( authToken, logLevel, resolver, encryptionEnabled, additionalConfig );
        }

        public InstanceConfig withAdditionalConfig( UnaryOperator<Config.ConfigBuilder> additionalConfig )
        {
            if ( this.additionalConfig != null )
            {
                throw new IllegalStateException( "cannot set additional config multiple times" );
            }
            return new InstanceConfig( authToken, logLevel, resolver, encryptionEnabled, additionalConfig );
        }

        public Config.ConfigBuilder fill( Config.ConfigBuilder builder )
        {
            if ( resolver != null )
            {
                builder = builder.withResolver( resolver );
            }
            if ( additionalConfig != null )
            {
                builder = additionalConfig.apply( builder );
            }
            builder = encryptionEnabled ? builder.withEncryption() : builder.withoutEncryption();
            return builder;
        }
    }
}
