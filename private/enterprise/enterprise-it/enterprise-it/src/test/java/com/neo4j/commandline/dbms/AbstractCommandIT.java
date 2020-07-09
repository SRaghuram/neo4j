/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.neo4j.configuration.Config.DEFAULT_CONFIG_FILE_NAME;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;

@ExtendWith( SuppressOutputExtension.class )
@EnterpriseDbmsExtension( configurationCallback = "configuration" )
@ResourceLock( Resources.SYSTEM_OUT )
abstract class AbstractCommandIT
{
    @Inject
    DatabaseManagementService managementService;
    @Inject
    GraphDatabaseAPI databaseAPI;
    @Inject
    TestDirectory testDirectory;
    @Inject
    Config config;
    @Inject
    Neo4jLayout neo4jLayout;
    @Inject
    FileSystemAbstraction fs;
    Path neo4jHome;
    Path configDir;

    @BeforeEach
    void setUp() throws IOException
    {
        Path dataDir = neo4jLayout.databasesDirectory();
        neo4jHome = config.get( GraphDatabaseSettings.neo4j_home );
        configDir = testDirectory.directoryPath( "configDir" );
        appendConfigSetting( databases_root_path, dataDir );
    }

    @ExtensionCallback
    void configuration( TestDatabaseManagementServiceBuilder builder )
    {   // no-op by default
    }

    protected <T> void appendConfigSetting( Setting<T> setting, T value ) throws IOException
    {
        Path configFile = configDir.resolve( DEFAULT_CONFIG_FILE_NAME );
        List<String> allSettings;
        if ( fs.fileExists( configFile.toFile() ) )
        {
            allSettings = Files.readAllLines( configFile );
        }
        else
        {
            allSettings = new ArrayList<>();
        }
        allSettings.add( formatProperty( setting, value ) );
        Files.write( configFile, allSettings );
    }

    private <T> String formatProperty( Setting<T> setting, T value )
    {
        String valueString = value.toString();
        if ( value instanceof Path )
        {
            valueString = value.toString().replace( '\\', '/' );
        }
        return format( "%s=%s", setting.name(), valueString );
    }
}
