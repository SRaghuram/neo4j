/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import com.neo4j.configuration.FabricEnterpriseSettings;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.helpers.HostnamePort;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.logging.internal.LogService;

public class TestServer implements AutoCloseable
{
    private final LifeSupport lifeSupport = new LifeSupport();
    private final List<Object> mocks = new ArrayList<>();
    private Dependencies dependencies;
    // instance of config that was supplied
    private Config config;
    // instance of config that was created by DBMS
    private Config runtimeConfig;
    private DatabaseManagementService dbms;
    private Path directory;
    private boolean databaseRootDirProvided;
    private TestFabricDatabaseManagementServiceBuilder dbmsBuilder;
    private LogService logService;

    public TestServer()
    {
        this( Config.newBuilder().build() );
    }

    public TestServer( Config config )
    {
        this.config = config;
    }

    public TestServer( Config config, Path databaseRootDir )
    {
        this( config );

        databaseRootDirProvided = true;
        directory = databaseRootDir;
    }

    public void addMocks( Object... mocks )
    {
        this.mocks.addAll( Arrays.asList( mocks ) );
    }

    public void addMocks( List<Object> mocks )
    {
        this.mocks.addAll( mocks );
    }

    public void setLogService( LogService logService )
    {
        this.logService = logService;
        addMocks( logService );
    }

    public void start()
    {
        if ( !databaseRootDirProvided )
        {
            this.directory = createDirectory();
        }
        var dbmsBuilder = new TestFabricDatabaseManagementServiceBuilder( directory, mocks );

        if ( logService != null && logService.getInternalLogProvider() != null )
        {
            dbmsBuilder.setInternalLogProvider( logService.getInternalLogProvider() );
        }
        if ( logService != null && logService.getUserLogProvider() != null )
        {
            dbmsBuilder.setUserLogProvider( logService.getUserLogProvider() );
        }
        this.dbms = dbmsBuilder.setConfig( config ).build();

        dependencies = dbmsBuilder.getDependencies();

        lifeSupport.start();

        runtimeConfig = dependencies.resolveDependency( Config.class );

        var hostPort = getHostnamePort();
        if ( hostPort != null )
        {
            runtimeConfig.setDynamic( FabricEnterpriseSettings.fabric_servers_setting, List.of( new SocketAddress( hostPort.getHost(), hostPort.getPort() ) ),
                    "TestServer" );
        }
    }

    public void stop()
    {
        try
        {
            dbms.shutdown();
            lifeSupport.shutdown();
            lifeSupport.stop();
        }
        finally
        {
            if ( !databaseRootDirProvided )
            {
                deleteDirectory( this.directory );
            }
        }
    }

    private Path createDirectory()
    {
        try
        {
            return Files.createTempDirectory( getClass().getSimpleName() );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    private void deleteDirectory( Path dir )
    {
        try
        {
            FileUtils.deletePathRecursively( dir );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    public void close()
    {
        stop();
    }

    public GraphDatabaseFacade createDatabase( String name )
    {
        dbms.createDatabase( name );
        return (GraphDatabaseFacade) dbms.database( name );
    }

    public DatabaseManagementService getDbms()
    {
        return dbms;
    }

    public Config getConfig()
    {
        return config;
    }

    public Dependencies getDependencies()
    {
        return dependencies;
    }

    public URI getBoltRoutingUri()
    {
        return getBoltUri( "neo4j" );
    }

    public URI getBoltDirectUri()
    {
        return getBoltUri( "bolt" );
    }

    private URI getBoltUri( String scheme )
    {
        HostnamePort hostPort = getHostnamePort();
        try
        {
            return new URI( scheme, null, hostPort.getHost(), hostPort.getPort(), null, null, null );
        }
        catch ( URISyntaxException x )
        {
            throw new IllegalArgumentException( x.getMessage(), x );
        }
    }

    public HostnamePort getHostnamePort()
    {
        var portRegister = dependencies.resolveDependency( ConnectorPortRegister.class );
        return portRegister.getLocalAddress( BoltConnector.NAME );
    }

    public Config getRuntimeConfig()
    {
        return runtimeConfig;
    }
}
