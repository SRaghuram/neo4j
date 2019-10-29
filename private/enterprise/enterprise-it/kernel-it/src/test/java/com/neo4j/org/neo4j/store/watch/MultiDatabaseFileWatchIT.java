/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.org.neo4j.store.watch;

import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.File;
import java.nio.file.WatchKey;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.neo4j.dbms.api.DatabaseExistsException;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.io.fs.watcher.FileWatchEventListener;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.util.watcher.FileSystemWatcherService;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.fs.FileUtils.deleteFile;

@TestDirectoryExtension
class MultiDatabaseFileWatchIT
{
    @Inject
    private TestDirectory testDirectory;
    private GraphDatabaseService database;
    private GraphDatabaseFacade firstContext;
    private GraphDatabaseFacade secondContext;
    private GraphDatabaseFacade thirdContext;
    private AssertableLogProvider logProvider;
    private DatabaseManagementService managementService;

    @BeforeEach
    void setUp() throws DatabaseExistsException
    {
        logProvider = new AssertableLogProvider( true );
        managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homeDir() )
                .setInternalLogProvider( logProvider )
                .build();
        database = managementService.database( DEFAULT_DATABASE_NAME );
        managementService.createDatabase( "first" );
        managementService.createDatabase( "second" );
        managementService.createDatabase( "third" );
        firstContext  = (GraphDatabaseFacade) managementService.database( "first" );
        secondContext = (GraphDatabaseFacade) managementService.database( "second" );
        thirdContext  = (GraphDatabaseFacade) managementService.database( "third" );
    }

    @AfterEach
    void tearDown()
    {
        managementService.shutdown();
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    void deleteFileInOneDatabaseWarnAboutThatParticularDatabase() throws InterruptedException
    {
        File firstDbMetadataStore = firstContext.databaseLayout().metadataStore();
        FileSystemWatcherService fileSystemWatcher = getFileSystemWatcher();
        DeletionLatchEventListener deletionListener = new DeletionLatchEventListener( firstDbMetadataStore.getName() );
        fileSystemWatcher.getFileWatcher().addFileWatchEventListener( deletionListener );
        deleteFile( firstDbMetadataStore );

        deletionListener.awaitDeletionNotification();

        logProvider.formattedMessageMatcher().assertContains( "'neostore' which belongs to the 'first' database was deleted while it was running." );
        logProvider.formattedMessageMatcher().assertNotContains( "'neostore' which belongs to the 'second' database was deleted while it was running." );
        logProvider.formattedMessageMatcher().assertNotContains( "'neostore' which belongs to the 'third' database was deleted while it was running." );
    }

    @Test
    @DisabledOnOs( OS.WINDOWS )
    void differentEventsGoToDifferentDatabaseListeners() throws InterruptedException
    {
        File firstDbMetadataStore = firstContext.databaseLayout().metadataStore();
        File secondDbNodeStore = secondContext.databaseLayout().nodeStore();
        File thirdDbRelStore = thirdContext.databaseLayout().relationshipStore();

        FileSystemWatcherService fileSystemWatcher = getFileSystemWatcher();
        DeletionLatchEventListener deletionListener = new DeletionLatchEventListener( thirdDbRelStore.getName(), secondDbNodeStore.getName(),
                firstDbMetadataStore.getName() );
        fileSystemWatcher.getFileWatcher().addFileWatchEventListener( deletionListener );

        deleteFile( firstDbMetadataStore );
        deleteFile( secondDbNodeStore );
        deleteFile( thirdDbRelStore );

        deletionListener.awaitDeletionNotification();

        logProvider.formattedMessageMatcher().assertContains( "'neostore' which belongs to the 'first' database was deleted while it was running." );
        logProvider.formattedMessageMatcher().assertContains(
                "'neostore.nodestore.db' which belongs to the 'second' database was deleted while it was running." );
        logProvider.formattedMessageMatcher().assertContains(
                "'neostore.relationshipstore.db' which belongs to the 'third' database was deleted while it was running." );
    }

    private FileSystemWatcherService getFileSystemWatcher()
    {
        return ((GraphDatabaseAPI) database).getDependencyResolver().resolveDependency( FileSystemWatcherService.class );
    }

    private static class DeletionLatchEventListener implements FileWatchEventListener
    {
        private final Set<String> expectedFileNames;
        private final CountDownLatch deletionLatch;

        DeletionLatchEventListener( String... expectedFileNames )
        {
            this.expectedFileNames = Set.of( expectedFileNames );
            this.deletionLatch = new CountDownLatch( expectedFileNames.length );
        }

        @Override
        public void fileDeleted( WatchKey key, String fileName )
        {
            if ( expectedFileNames.contains( fileName ) )
            {
                deletionLatch.countDown();
            }
        }

        void awaitDeletionNotification() throws InterruptedException
        {
            deletionLatch.await();
        }
    }
}
