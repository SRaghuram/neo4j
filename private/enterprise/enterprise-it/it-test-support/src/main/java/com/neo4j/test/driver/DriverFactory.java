/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test.driver;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.driver.net.ServerAddressResolver;
import org.neo4j.logging.Log;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.io.IOUtils.closeAll;

public class DriverFactory implements Closeable
{
    private final AtomicLong driverCounter = new AtomicLong();
    private final Collection<AutoCloseable> closeableCollection = ConcurrentHashMap.newKeySet();
    private final TestDirectory testDirectory;
    static final String LOGS_DIR = "driver-logs";

    DriverFactory( TestDirectory testDirectory )
    {
        this.testDirectory = testDirectory;
    }

    private Log createNewLogFile() throws IOException
    {
        var fs = testDirectory.getFileSystem();
        var logDir = new File( testDirectory.absolutePath(), LOGS_DIR );
        fs.mkdir( logDir );
        var outputStream = fs.openAsOutputStream( new File( logDir, "driver-" + driverCounter.incrementAndGet() + ".log" ), false );
        closeableCollection.add( outputStream );
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
        return addCloseable( GraphDatabase.driver( uri, createDefaultConfigBuilder().build() ) );
    }

    public Driver graphDatabaseDriver( String uri ) throws IOException
    {
        return addCloseable( GraphDatabase.driver( uri, createDefaultConfigBuilder().build() ) );
    }

    public Driver graphDatabaseDriver( ServerAddressResolver resolver ) throws IOException
    {
        return addCloseable( GraphDatabase.driver( "neo4j://ignore.com", createDefaultConfigBuilder().withResolver( resolver ).build() ) );
    }

    @Override
    public void close() throws IOException
    {
        closeAll( closeableCollection );
        closeableCollection.clear();
    }

    private Driver addCloseable( Driver driver )
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
