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
package org.neo4j.db;

import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.api.DatabaseManagementServiceBuilder;
import org.neo4j.dbms.database.DatabaseContext;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.DatabaseShutdownException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.factory.DatabaseInfo;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.storemigration.StoreUpgrader;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.Exceptions.findCauseOrSuppressed;
import static org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory.createPageCache;

@EphemeralTestDirectoryExtension
class DatabaseStartupTest
{
    @Inject
    FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    @Test
    void startTheDatabaseWithWrongVersionShouldFailWithUpgradeNotAllowed() throws Throwable
    {
        // given
        // create a store
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        File storeDirectory = testDirectory.storeDir();
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( storeDirectory ).build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode();
            tx.commit();
        }
        managementService.shutdown();

        // mess up the version in the metadatastore
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              ThreadPoolJobScheduler scheduler = new ThreadPoolJobScheduler();
              PageCache pageCache = createPageCache( fileSystem, scheduler ) )
        {
            MetaDataStore.setRecord( pageCache, databaseLayout.metadataStore(),
                    MetaDataStore.Position.STORE_VERSION, MetaDataStore.versionStringToLong( "bad" ));
        }

        managementService = new TestDatabaseManagementServiceBuilder( storeDirectory ).build();
        GraphDatabaseService databaseService = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            assertThrows( DatabaseShutdownException.class, databaseService::beginTx );
            DatabaseManager<?> databaseManager = getDatabaseManager( (GraphDatabaseAPI) databaseService );
            DatabaseContext databaseContext = databaseManager.getDatabaseContext( databaseLayout.getDatabaseName() ).get();
            assertTrue( databaseContext.isFailed() );
            Throwable throwable = findCauseOrSuppressed( databaseContext.failureCause(), e -> e instanceof IllegalArgumentException ).get();
            assertEquals( "Unknown store version 'bad'", throwable.getMessage() );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void startTheDatabaseWithWrongVersionShouldFailAlsoWhenUpgradeIsAllowed() throws Throwable
    {
        // given
        // create a store
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        File storeDirectory = testDirectory.storeDir();
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( storeDirectory ).build();
        GraphDatabaseService db = managementService.database( DEFAULT_DATABASE_NAME );
        try ( Transaction tx = db.beginTx() )
        {
            tx.createNode();
            tx.commit();
        }
        managementService.shutdown();

        // mess up the version in the metadatastore
        String badStoreVersion = "bad";
        try ( FileSystemAbstraction fileSystem = new DefaultFileSystemAbstraction();
              ThreadPoolJobScheduler scheduler = new ThreadPoolJobScheduler();
              PageCache pageCache = createPageCache( fileSystem, scheduler ) )
        {
            MetaDataStore.setRecord( pageCache, databaseLayout.metadataStore(), MetaDataStore.Position.STORE_VERSION,
                    MetaDataStore.versionStringToLong( badStoreVersion ) );
        }

        managementService = new TestDatabaseManagementServiceBuilder( storeDirectory )
                .setConfig( GraphDatabaseSettings.allow_upgrade, true )
                .build();
        GraphDatabaseService databaseService = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            assertThrows( DatabaseShutdownException.class, databaseService::beginTx );
            DatabaseManager<?> databaseManager = getDatabaseManager( (GraphDatabaseAPI) databaseService );
            DatabaseContext databaseContext = databaseManager.getDatabaseContext( databaseLayout.getDatabaseName() ).get();
            assertTrue( databaseContext.isFailed() );
            Optional<Throwable> upgradeException =
                    findCauseOrSuppressed( databaseContext.failureCause(), e -> e instanceof StoreUpgrader.UnexpectedUpgradingStoreVersionException );
            assertTrue( upgradeException.isPresent() );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    @Test
    void startTestDatabaseOnProvidedNonAbsoluteFile()
    {
        File directory = new File( "notAbsoluteDirectory" );
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( directory ).impermanent().build();
        managementService.shutdown();
    }

    @Test
    void startCommunityDatabaseOnProvidedNonAbsoluteFile()
    {
        File directory = new File( "notAbsoluteDirectory" );
        EphemeralCommunityManagementServiceFactory factory = new EphemeralCommunityManagementServiceFactory();
        DatabaseManagementServiceBuilder databaseFactory = new EphemeralDatabaseManagementServiceBuilder(  directory, factory );
        DatabaseManagementService managementService = databaseFactory.build();
        GraphDatabaseService service = managementService.database( DEFAULT_DATABASE_NAME );
        managementService.shutdown();
    }

    @Test
    void dumpSystemDiagnosticLoggingOnStartup()
    {
        AssertableLogProvider logProvider = new AssertableLogProvider();
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder( testDirectory.storeDir() )
                .setInternalLogProvider( logProvider )
                .build();
        GraphDatabaseService database = managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            logProvider.rawMessageMatcher().assertContains( "System diagnostics" );
            logProvider.rawMessageMatcher().assertContains( "System memory information" );
            logProvider.rawMessageMatcher().assertContains( "JVM memory information" );
            logProvider.rawMessageMatcher().assertContains( "Operating system information" );
            logProvider.rawMessageMatcher().assertContains( "JVM information" );
            logProvider.rawMessageMatcher().assertContains( "Java classpath" );
            logProvider.rawMessageMatcher().assertContains( "Library path" );
            logProvider.rawMessageMatcher().assertContains( "System properties" );
            logProvider.rawMessageMatcher().assertContains( "(IANA) TimeZone database version" );
            logProvider.rawMessageMatcher().assertContains( "Network information" );
            logProvider.rawMessageMatcher().assertContains( "DBMS config" );
        }
        finally
        {
            managementService.shutdown();
        }
    }

    private static DatabaseManager<?> getDatabaseManager( GraphDatabaseAPI databaseService )
    {
        return databaseService.getDependencyResolver().resolveDependency( DatabaseManager.class );
    }

    private static class EphemeralCommunityManagementServiceFactory extends DatabaseManagementServiceFactory
    {
        EphemeralCommunityManagementServiceFactory()
        {
            super( DatabaseInfo.COMMUNITY, CommunityEditionModule::new );
        }

        @Override
        protected GlobalModule createGlobalModule( File storeDir, Config config, ExternalDependencies dependencies )
        {
            return new GlobalModule( storeDir, config, databaseInfo, dependencies )
            {
                @Override
                protected FileSystemAbstraction createFileSystemAbstraction()
                {
                    return new EphemeralFileSystemAbstraction();
                }
            };
        }
    }

    private static class EphemeralDatabaseManagementServiceBuilder extends DatabaseManagementServiceBuilder
    {
        private final EphemeralCommunityManagementServiceFactory factory;

        EphemeralDatabaseManagementServiceBuilder( File databaseRootDir, EphemeralCommunityManagementServiceFactory factory )
        {
            super( databaseRootDir );
            this.factory = factory;
        }

        @Override
        protected DatabaseManagementService newDatabaseManagementService( File storeDir, Config config, ExternalDependencies dependencies )
        {
            return factory.build( storeDir, augmentConfig( config ), dependencies );
        }
    }
}
