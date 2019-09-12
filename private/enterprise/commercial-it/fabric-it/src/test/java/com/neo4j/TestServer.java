/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.lifecycle.LifeSupport;

public class TestServer implements AutoCloseable
{
    private final LifeSupport lifeSupport = new LifeSupport();
    private final List<Object> mocks = new ArrayList<>();
    private Dependencies dependencies;
    private Config config;
    private DatabaseManagementService dbms;
    private Path directory;

    public TestServer()
    {
        this( Config.newBuilder().build() );
    }

    public TestServer( Config config )
    {
        this.config = config;
    }

    public void addMocks( Object... mocks )
    {
        this.mocks.addAll( Arrays.asList( mocks ) );
    }

    public void start()
    {
        this.directory = createDirectory();
        var dbmsBuilder = new TestFabricDatabaseManagementServiceBuilder( directory.toFile(), mocks );
        this.dbms = dbmsBuilder.setConfig( config )
                .setConfig( GraphDatabaseSettings.auth_enabled , "false" )
                .build();

        dependencies = dbmsBuilder.getDependencies();

        lifeSupport.start();
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
            deleteDirectory( this.directory );
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

    public Config getConfig()
    {
        return config;
    }

    public Dependencies getDependencies()
    {
        return dependencies;
    }
}
