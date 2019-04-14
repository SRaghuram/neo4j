/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.dbms;

import com.neo4j.commercial.edition.factory.CommercialGraphDatabaseFactory;
import com.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.util.Set;

import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.Settings;
import org.neo4j.dbms.database.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;

@ExtendWith( TestDirectoryExtension.class )
class DefaultDatabaseSelectionIT
{
    private static final String LEGACY_DATABASE_NAME = "graph.db";
    @Inject
    private TestDirectory testDirectory;
    private DatabaseManagementService managementService;

    @Test
    void startWithSystemAndNeo4jByDefault()
    {
        GraphDatabaseService database = startDatabase();
        try
        {
            checkDatabaseNames( database, "neo4j" );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void startWithSystemAndDefaultDatabase()
    {
        String customDefaultDatabase = "customDefaultDatabase";
        GraphDatabaseService database = startDatabase( customDefaultDatabase );
        try
        {
            checkDatabaseNames( database, customDefaultDatabase );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void startWithSystemAndLegacyDbOnTopOfExistingDatabase() throws IOException
    {
        prepareLegacyStandalone( LEGACY_DATABASE_NAME );

        GraphDatabaseService database = startDatabase( LEGACY_DATABASE_NAME );
        try
        {
            checkDatabaseNames( database, LEGACY_DATABASE_NAME );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void startWithSystemAndDefaultWhenLegacyExistButCustomDefaultDbIsConfigured() throws IOException
    {
        prepareLegacyStandalone( LEGACY_DATABASE_NAME );

        String customDatabase = "custom";
        GraphDatabaseService database = startDatabase( customDatabase );
        try
        {
            checkDatabaseNames( database, customDatabase );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void startWithSystemAndLegacyConfiguredActiveDatabase()
    {
        String customDbName = "activeDb";
        DatabaseManagementService managementService = getDatabaseBuilder().setConfig( "dbms.active_database", customDbName ).newDatabaseManagementService();
        GraphDatabaseService database = managementService.database( customDbName );
        try
        {
            checkDatabaseNames( database, customDbName );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void startWithSystemAndLegacyConfiguredActiveDatabaseEvenWhenDefaultLegacyExist() throws IOException
    {
        prepareLegacyStandalone( LEGACY_DATABASE_NAME );
        String customDbName = "legacyCustomDb";
        DatabaseManagementService managementService = getDatabaseBuilder().setConfig( "dbms.active_database", customDbName ).newDatabaseManagementService();
        GraphDatabaseService database = managementService.database( customDbName );
        try
        {
            checkDatabaseNames( database, customDbName );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private static void checkDatabaseNames( GraphDatabaseService database, String defaultDatabaseName )
    {
        DatabaseManager<?> databaseManager = getDatabaseManager( database );
        Set<DatabaseId> databases = databaseManager.registeredDatabases().keySet();
        assertThat( databases, containsInAnyOrder( new DatabaseId( defaultDatabaseName ), new DatabaseId( "system" ) ) );
    }

    private void prepareLegacyStandalone( String databaseName ) throws IOException
    {
        GraphDatabaseService database = startDatabase( databaseName );
        managementService.shutdown();
        DatabaseLayout systemLayout = testDirectory.databaseLayout( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
        assertTrue( systemLayout.metadataStore().exists() );
        FileSystemAbstraction fileSystem = testDirectory.getFileSystem();
        fileSystem.deleteRecursively( systemLayout.getTransactionLogsDirectory() );
        fileSystem.deleteRecursively( systemLayout.databaseDirectory() );
        assertFalse( systemLayout.databaseDirectory().exists() );
    }

    private GraphDatabaseService startDatabase()
    {
        managementService = getDatabaseBuilder().newDatabaseManagementService();
        return managementService.database( DEFAULT_DATABASE_NAME );
    }

    private GraphDatabaseService startDatabase( String databaseName )
    {
        managementService = getDatabaseBuilder()
               .setConfig( GraphDatabaseSettings.default_database, databaseName ).newDatabaseManagementService();
        return managementService.database( databaseName );
    }

    private GraphDatabaseBuilder getDatabaseBuilder()
    {
        return new CommercialGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( testDirectory.storeDir() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
    }

    private static DatabaseManager<?> getDatabaseManager( GraphDatabaseService database )
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }
}
