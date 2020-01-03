/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commandline.dbms;

import com.neo4j.test.extension.EnterpriseDbmsExtension;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.rule.TestDirectory;

import static java.lang.String.format;
import static java.util.Collections.singletonList;
import static org.neo4j.configuration.Config.DEFAULT_CONFIG_FILE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.databases_root_path;

@ExtendWith( SuppressOutputExtension.class )
@EnterpriseDbmsExtension
abstract class AbstractCommandIT
{
    @Inject
    DatabaseManagementService managementService;
    @Inject
    GraphDatabaseAPI databaseAPI;
    @Inject
    TestDirectory testDirectory;
    Path neo4jHome;
    Path configDir;

    @BeforeEach
    void setUp() throws IOException
    {
        Config config = databaseAPI.getDependencyResolver().resolveDependency( Config.class );
        File dataDir = databaseAPI.databaseLayout().getNeo4jLayout().databasesDirectory();
        neo4jHome = config.get( GraphDatabaseSettings.neo4j_home );
        configDir = testDirectory.directory( "configDir" ).toPath();
        Files.write( configDir.resolve( DEFAULT_CONFIG_FILE_NAME ), singletonList( formatProperty( databases_root_path, dataDir.toPath() ) ) );
    }

    private static String formatProperty( Setting<?> setting, Path path )
    {
        return format( "%s=%s", setting.name(), path.toString().replace( '\\', '/' ) );
    }
}
