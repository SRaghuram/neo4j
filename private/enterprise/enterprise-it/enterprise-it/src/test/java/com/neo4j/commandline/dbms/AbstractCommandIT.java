/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeEach;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.cli.ExecutionContext;
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
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static org.neo4j.configuration.Config.DEFAULT_CONFIG_FILE_NAME;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.databases_root_path;

@EnterpriseDbmsExtension( configurationCallback = "configuration" )
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

    Output out;
    Output err;

    @BeforeEach
    void setUp() throws IOException
    {
        Path dataDir = neo4jLayout.databasesDirectory();
        neo4jHome = config.get( GraphDatabaseSettings.neo4j_home );
        configDir = testDirectory.directory( "configDir" );
        appendConfigSetting( databases_root_path, dataDir );
        out = new Output();
        err = new Output();
    }

    @ExtensionCallback
    void configuration( TestDatabaseManagementServiceBuilder builder )
    {   // no-op by default
    }

    static class Output
    {
        private final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        final PrintStream printStream = new PrintStream( buffer );

        public boolean containsMessage( String message )
        {
            return toString().contains( message );
        }

        @Override
        public String toString()
        {
            return buffer.toString( StandardCharsets.UTF_8 );
        }
    }
    protected ExecutionContext getExtensionContext()
    {
        return new ExecutionContext( neo4jHome, configDir, out.printStream, err.printStream, fs );
    }

    protected <T> void appendConfigSetting( Setting<T> setting, T value ) throws IOException
    {
        Path configFile = configDir.resolve( DEFAULT_CONFIG_FILE_NAME );
        List<String> allSettings;
        if ( fs.fileExists( configFile ) )
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
