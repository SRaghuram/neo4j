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
package org.neo4j.harness.internal;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.util.Collection;
import java.util.Optional;

import org.neo4j.configuration.ConfigUtils;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.Connector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.config.Configuration;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.NeoServer;

public class InProcessNeo4j implements Neo4j, AutoCloseable
{
    private static final String DEFAULT_BOLT_CONNECTOR_KEY = "bolt";

    private final File serverFolder;
    private final File userLogFile;
    private final File internalLogFile;
    private final NeoServer server;
    private final Closeable additionalClosable;
    private ConnectorPortRegister connectorPortRegister;

    InProcessNeo4j( File serverFolder, File userLogFile, File internalLogFile, NeoServer server, Closeable additionalClosable )
    {
        this.serverFolder = serverFolder;
        this.userLogFile = userLogFile;
        this.internalLogFile = internalLogFile;
        this.server = server;
        this.additionalClosable = additionalClosable;
    }

    @Override
    public URI boltURI()
    {
        Collection<BoltConnector> connectors = ConfigUtils.getEnabledBoltConnectors( server.getConfig() );

        BoltConnector defaultConnector = null;
        BoltConnector firstConnector = null;

        for ( BoltConnector connector : connectors )
        {
            if ( DEFAULT_BOLT_CONNECTOR_KEY.equals( connector.name() ) )
            {
                defaultConnector = connector;
            }
            if ( firstConnector == null )
            {
                firstConnector = connector;
            }
        }

        if ( defaultConnector != null )
        {
            // bolt connector with default key is configured, return its address
            return connectorUri( DEFAULT_BOLT_CONNECTOR_KEY, defaultConnector );
        }
        if ( firstConnector != null )
        {
            // some bolt connector is configured, return its address
            return connectorUri( DEFAULT_BOLT_CONNECTOR_KEY, firstConnector );
        }

        throw new IllegalStateException( "Bolt connector is not configured" );
    }

    @Override
    public URI httpURI()
    {
        return httpConnectorUri( "http" )
                .orElseThrow( () -> new IllegalStateException( "HTTP connector is not configured" ) );
    }

    @Override
    public URI httpsURI()
    {
        return httpsConnectorUri( "https" )
                .orElseThrow( () -> new IllegalStateException( "HTTPS connector is not configured" ) );
    }

    public void start()
    {
        this.server.start();
        this.connectorPortRegister = connectorPortRegister( server );
    }

    @Override
    public void close()
    {
        server.stop();
        this.connectorPortRegister = null;
        try
        {
            additionalClosable.close();
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
        try
        {
            if ( looksLikeMd5Hash( serverFolder.getName() ) )
            {
                FileUtils.deleteRecursively( serverFolder );
            }
        }
        catch ( IOException e )
        {
            throw new RuntimeException( "Failed to clean up test server directory.", e );
        }
    }

    @Override
    public void printLogs( PrintStream out )
    {
        printLog( "User Log File", userLogFile, out );
        printLog( "Internal Log File", internalLogFile, out );
    }

    private static void printLog( String description, File file, PrintStream out )
    {
        if ( file != null && file.exists() )
        {
            out.println( String.format( "---------- BEGIN %s ----------", description ) );

            try ( BufferedReader reader = new BufferedReader( new FileReader( file ) ) )
            {
                reader.lines().forEach( out::println );
            }
            catch ( IOException ex )
            {
                out.println( "Unable to collect log files: " + ex.getMessage() );
                ex.printStackTrace( out );
            }
            finally
            {
                out.println( String.format( "---------- END %s ----------", description ) );
            }
        }
    }

    private boolean looksLikeMd5Hash( String name )
    {
        // Pure paranoia, and a silly check - but this decreases the likelihood that we delete something that isn't
        // our randomly generated folder significantly.
        return name.length() == 32;
    }

    @Override
    public GraphDatabaseService graph()
    {
        return server.getDatabaseService().getDatabase();
    }

    @Override
    public Configuration config()
    {
        return server.getConfig();
    }

    private Optional<URI> httpConnectorUri( String scheme )
    {
        return ConfigUtils.getEnabledHttpConnectors( server.getConfig() ).stream().findFirst().map( connector -> connectorUri( scheme, connector ) );
    }

    private Optional<URI> httpsConnectorUri( String scheme )
    {
        return ConfigUtils.getEnabledHttpsConnectors( server.getConfig() ).stream().findFirst().map( connector -> connectorUri( scheme, connector ) );
    }

    private URI connectorUri( String scheme, Connector connector )
    {
        HostnamePort hostPort = connectorPortRegister.getLocalAddress( connector.name() );
        return URI.create( scheme + "://" + hostPort + "/" );
    }

    private static ConnectorPortRegister connectorPortRegister( NeoServer server )
    {
        return ((GraphDatabaseAPI) server.getDatabaseService().getDatabase()).getDependencyResolver().resolveDependency( ConnectorPortRegister.class );
    }
}
