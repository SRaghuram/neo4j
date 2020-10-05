/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.driver;

import com.neo4j.causalclustering.common.Cluster;

import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
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
import org.neo4j.logging.Log;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.io.IOUtils.closeAll;

public class DriverFactory implements Closeable
{
    private static final URI BOOTSTRAP_URI = URI.create( "neo4j://ignore.com" );
    static final String LOGS_DIR = "driver-logs";

    private final AtomicLong driverCounter = new AtomicLong();
    private final LinkedList<AutoCloseable> closeableCollection = new LinkedList<>();
    private final TestDirectory testDirectory;

    private AuthToken authToken = AuthTokens.none();

    DriverFactory( TestDirectory testDirectory )
    {
        this.testDirectory = testDirectory;
    }

    public void setAuthToken( AuthToken authToken )
    {
        this.authToken = authToken;
    }

    private Log createNewLogFile() throws IOException
    {
        var fs = testDirectory.getFileSystem();
        var logDir = testDirectory.homePath().resolve( LOGS_DIR );
        fs.mkdir( logDir );
        var outputStream = fs.openAsOutputStream( logDir.resolve( "driver-" + driverCounter.incrementAndGet() + ".log" ), false );
        addCloseable( outputStream );
        return new Log4jLogProvider( outputStream ).getLog( getClass() );
    }

    private Logging createLogger( Log log )
    {
        return name -> new TranslatedLogger( log );
    }

    private Config.ConfigBuilder createDefaultConfigBuilder() throws IOException
    {
        return Config.builder().withoutEncryption().withLogging( createLogger( createNewLogFile() ) );
    }

    public Driver graphDatabaseDriver( URI uri ) throws IOException
    {
        return addCloseable( getDriver( uri ) );
    }

    public Driver graphDatabaseDriver( String uri ) throws IOException
    {
        return addCloseable( getDriver( URI.create( uri ) ) );
    }

    public Driver graphDatabaseDriver( Collection<URI> boltURIs ) throws IOException
    {
        return graphDatabaseDriver( address -> address.host().equals( BOOTSTRAP_URI.getHost() )
                                               ? boltURIs.stream().map( uri -> ServerAddress.of( uri.getHost(), uri.getPort() ) ).collect( Collectors.toSet() )
                                               : Set.of( address ) );
    }

    public Driver graphDatabaseDriver( ServerAddressResolver resolver ) throws IOException
    {
        return addCloseable( GraphDatabase.driver( BOOTSTRAP_URI, authToken, createDefaultConfigBuilder().withResolver( resolver ).build() ) );
    }

    public ClusterChecker clusterChecker( Collection<URI> boltURIs ) throws IOException
    {
        return addCloseable( ClusterChecker.fromBoltURIs( boltURIs, this::getDriver ) );
    }

    public ClusterChecker clusterChecker( Cluster cluster ) throws IOException
    {
        List<URI> coreUris = cluster.coreMembers()
                                    .stream()
                                    .filter( c -> !c.isShutdown() )
                                    .map( c -> URI.create( c.directURI() ) )
                                    .collect( Collectors.toList() );
        return clusterChecker( coreUris );
    }

    private Driver getDriver( URI uri1 ) throws IOException
    {
        return GraphDatabase.driver( uri1, authToken, createDefaultConfigBuilder().build() );
    }

    @Override
    public void close() throws IOException
    {
        // Shut down AutoCloseables in reverse order from the order they were created.
        //
        // Why? Here's a specific example. We do this:
        //
        //        addCloseable(new Driver(logFile=addCloseable(new FileOutputStream("my.log")))
        //
        // the List<AutoCloseable> looks like:
        //
        //        1: FileOutputStream@my.log
        //        2: Driver(log=FileOutputStream@my.log)
        //
        // if we close the FileOutputStream *first* when the driver tries to write to the log during its own close() method an error is thrown :-(

        closeAll( getCloseablesInReverseOrder() );
    }

    private synchronized AutoCloseable[] getCloseablesInReverseOrder()
    {
        var output = new AutoCloseable[closeableCollection.size()];
        int i = 0;
        while ( !closeableCollection.isEmpty() )
        {
            output[i++] = closeableCollection.removeLast();
        }
        return output;
    }

    private synchronized <T extends AutoCloseable> T addCloseable( T driver )
    {
        closeableCollection.add( driver );
        return driver;
    }

    private static final class TranslatedLogger implements Logger
    {

        private final Log log;

        private TranslatedLogger( Log log )
        {
            this.log = log;
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
            return true;
        }
    }
}
