/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.configuration.OnlineBackupSettings;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.helpers.NormalizedDatabaseName;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseNotFoundException;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.default_database;

@Neo4jLayoutExtension
class DefaultDatabaseSelectionIT
{
    private static final String LEGACY_DATABASE_NAME = "graph.db";
    @Inject
    private Neo4jLayout neo4jLayout;
    @Inject
    private FileSystemAbstraction fileSystem;
    private DatabaseManagementService managementService;

    @AfterEach
    void tearDown()
    {
        if ( managementService != null )
        {
            managementService.shutdown();
        }
    }

    @Test
    void startWithSystemAndNeo4jByDefault()
    {
        GraphDatabaseService database = startDatabase();
        checkDatabaseNames( database, "neo4j" );
    }

    @Test
    void startWithSystemAndDefaultDatabase()
    {
        String customDefaultDatabase = "customDefaultDatabase";
        GraphDatabaseService database = startDatabase( customDefaultDatabase );
        checkDatabaseNames( database, customDefaultDatabase );
    }

    @Test
    void startWithSystemAndLegacyDbOnTopOfExistingDatabase() throws IOException
    {
        prepareLegacyStandalone( LEGACY_DATABASE_NAME );

        GraphDatabaseService database = startDatabase( LEGACY_DATABASE_NAME );
        checkDatabaseNames( database, LEGACY_DATABASE_NAME );
    }

    @Test
    void startWithSystemAndDefaultWhenLegacyExistButCustomDefaultDbIsConfigured() throws IOException
    {
        prepareLegacyStandalone( LEGACY_DATABASE_NAME );

        String customDatabase = "custom";
        GraphDatabaseService database = startDatabase( customDatabase );
        checkDatabaseNames( database, customDatabase );
        managementService.shutdown();
    }

    @Test
    void startWithSystemAndLegacyConfiguredActiveDatabase()
    {
        String customDbName = "activeDb";
        managementService = getDatabaseBuilder()
                .setConfigRaw( Map.of( "dbms.active_database", customDbName ) )
                .build();
        GraphDatabaseService database = managementService.database( customDbName );
        checkDatabaseNames( database, customDbName );
    }

    @Test
    void startWithSystemAndLegacyConfiguredActiveDatabaseEvenWhenDefaultLegacyExist() throws IOException
    {
        prepareLegacyStandalone( LEGACY_DATABASE_NAME );
        String customDbName = "legacyCustomDb";
        managementService = getDatabaseBuilder()
                .setConfigRaw( Map.of( "dbms.active_database", customDbName ) )
                .build();
        GraphDatabaseService database = managementService.database( customDbName );
        checkDatabaseNames( database, customDbName );
    }

    @Test
    void restartWithLegacyDatabasesAndIgnoreIt() throws IOException
    {
        managementService = createManagementService();
        checkDatabaseNames( managementService.database( SYSTEM_DATABASE_NAME ), "neo4j" );
        DatabaseLayout neo4j = (getDatabaseApiByName( "neo4j" )).databaseLayout();
        managementService.shutdown();

        DatabaseLayout legacyDbLayout = neo4jLayout.databaseLayout( LEGACY_DATABASE_NAME );
        copyDatabaseToLegacyDatabase( neo4j, legacyDbLayout );
        managementService = createManagementService();
        Config systemConfig = getDatabaseApiByName( SYSTEM_DATABASE_NAME ).getDependencyResolver().resolveDependency( Config.class );
        assertEquals( "neo4j", systemConfig.get( default_database ) );
        assertThrows( DatabaseNotFoundException.class, () -> managementService.database( LEGACY_DATABASE_NAME ) );
    }

    @Test
    void restartWithChangedDefaultDatabase()
    {
        managementService = createManagementService();
        checkDatabaseNames( managementService.database( SYSTEM_DATABASE_NAME ), "neo4j" );
        managementService.shutdown();

        String customDbName = "newBraveDatabase";
        managementService = getDatabaseBuilder()
                .setConfig( default_database, customDbName )
                .build();
        checkDatabaseNames( managementService.database( SYSTEM_DATABASE_NAME ), customDbName );
    }

    private GraphDatabaseAPI getDatabaseApiByName( String systemDatabaseName )
    {
        return (GraphDatabaseAPI) managementService.database( systemDatabaseName );
    }

    private void copyDatabaseToLegacyDatabase( DatabaseLayout neo4j, DatabaseLayout legacyDbLayout ) throws IOException
    {
        fileSystem.copyRecursively( neo4j.databaseDirectory().toFile(), legacyDbLayout.databaseDirectory().toFile() );
        fileSystem.copyRecursively( neo4j.getTransactionLogsDirectory().toFile(), legacyDbLayout.getTransactionLogsDirectory().toFile() );
    }

    private static void checkDatabaseNames( GraphDatabaseService database, String databaseName )
    {
        DatabaseManager<?> databaseManager = getDatabaseManager( database );
        Set<String> databases = databaseManager.registeredDatabases().keySet().stream().map( NamedDatabaseId::name ).collect( Collectors.toSet() );
        assertThat( databases ).contains( new NormalizedDatabaseName( databaseName ).name() );
        assertThat( databases ).contains( SYSTEM_DATABASE_NAME );
    }

    private void prepareLegacyStandalone( String databaseName ) throws IOException
    {
        startDatabase( databaseName );
        managementService.shutdown();
        DatabaseLayout systemLayout = neo4jLayout.databaseLayout( SYSTEM_DATABASE_NAME );
        assertTrue( Files.exists( systemLayout.metadataStore() ) );
        fileSystem.deleteRecursively( systemLayout.getTransactionLogsDirectory().toFile() );
        fileSystem.deleteRecursively( systemLayout.databaseDirectory().toFile() );
        assertFalse( Files.exists( systemLayout.databaseDirectory() ) );
    }

    private GraphDatabaseService startDatabase()
    {
        managementService = createManagementService();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private DatabaseManagementService createManagementService()
    {
        return getDatabaseBuilder().build();
    }

    private GraphDatabaseService startDatabase( String databaseName )
    {
        managementService = getDatabaseBuilder().setConfig( default_database, databaseName ).build();
        return managementService.database( databaseName );
    }

    private TestEnterpriseDatabaseManagementServiceBuilder getDatabaseBuilder()
    {
        return new TestEnterpriseDatabaseManagementServiceBuilder( neo4jLayout )
                .setConfig( OnlineBackupSettings.online_backup_enabled, false );
    }

    private static DatabaseManager<?> getDatabaseManager( GraphDatabaseService database )
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }
}
