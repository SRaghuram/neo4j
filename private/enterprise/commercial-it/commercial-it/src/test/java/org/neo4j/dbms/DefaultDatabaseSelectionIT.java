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
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.dbms.database.StandaloneDatabaseContext;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( TestDirectoryExtension.class )
class DefaultDatabaseSelectionIT
{
    private static final String LEGACY_DATABASE_NAME = "graph.db";
    @Inject
    private TestDirectory testDirectory;

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
            database.shutdown();
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
            database.shutdown();
        }
    }

    @Test
    void startWithSystemAndLegacyDbOnTopOfExistingDatabase() throws IOException
    {
        prepareLegacyStandalone( LEGACY_DATABASE_NAME );

        GraphDatabaseService database = startDatabase();
        try
        {
            checkDatabaseNames( database, LEGACY_DATABASE_NAME );
        }
        finally
        {
            database.shutdown();
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
            database.shutdown();
        }
    }

    @Test
    void startWithSystemAndLegacyConfiguredActiveDatabase()
    {
        String customDbName = "activeDb";
        GraphDatabaseService database = getDatabaseBuilder().setConfig( "dbms.active_database", customDbName ).newGraphDatabase();
        try
        {
            checkDatabaseNames( database, customDbName );
        }
        finally
        {
            database.shutdown();
        }
    }

    @Test
    void startWithSystemAndLegacyConfiguredActiveDatabaseEvenWhenDefaultLegacyExist() throws IOException
    {
        prepareLegacyStandalone( LEGACY_DATABASE_NAME );
        String customDbName = "legacyCustomDb";
        GraphDatabaseService database = getDatabaseBuilder().setConfig( "dbms.active_database", customDbName ).newGraphDatabase();
        try
        {
            checkDatabaseNames( database, customDbName );
        }
        finally
        {
            database.shutdown();
        }
    }

    private void checkDatabaseNames( GraphDatabaseService database, String defaultDatabaseName )
    {
        DatabaseManager<StandaloneDatabaseContext> databaseManager = getDatabaseManager( database );
        Set<String> databases = databaseManager.registeredDatabases().keySet();
        assertThat( databases, containsInAnyOrder( defaultDatabaseName, "system" ) );
    }

    private void prepareLegacyStandalone( String databaseName ) throws IOException
    {
        GraphDatabaseService database = startDatabase( databaseName );
        database.shutdown();
        DatabaseLayout systemLayout = testDirectory.storeLayout().databaseLayout( GraphDatabaseSettings.SYSTEM_DATABASE_NAME );
        assertTrue( systemLayout.metadataStore().exists() );
        FileSystemAbstraction fileSystem = testDirectory.getFileSystem();
        fileSystem.deleteRecursively( systemLayout.getTransactionLogsDirectory() );
        fileSystem.deleteRecursively( systemLayout.databaseDirectory() );
        assertFalse( systemLayout.databaseDirectory().exists() );
    }

    private GraphDatabaseService startDatabase()
    {
        return getDatabaseBuilder().newGraphDatabase();
    }

    private GraphDatabaseService startDatabase( String databaseName )
    {
        return getDatabaseBuilder()
               .setConfig( GraphDatabaseSettings.default_database, databaseName )
               .newGraphDatabase();
    }

    private GraphDatabaseBuilder getDatabaseBuilder()
    {
        return new CommercialGraphDatabaseFactory()
                .newEmbeddedDatabaseBuilder( testDirectory.databaseDir() )
                .setConfig( OnlineBackupSettings.online_backup_enabled, Settings.FALSE );
    }

    @SuppressWarnings( "unchecked" )
    private DatabaseManager<StandaloneDatabaseContext> getDatabaseManager( GraphDatabaseService database )
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( DatabaseManager.class );
    }
}
