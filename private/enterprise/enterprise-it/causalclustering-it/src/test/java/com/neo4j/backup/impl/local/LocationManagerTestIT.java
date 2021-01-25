/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.backup.impl.local;

import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.UUID;

import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static com.neo4j.configuration.OnlineBackupSettings.online_backup_enabled;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.logging.NullLogProvider.nullLogProvider;

@TestDirectoryExtension
@PageCacheExtension
class LocationManagerTestIT
{

    @Inject
    TestDirectory testDirectory;
    @Inject
    PageCache pageCache;

    Neo4jLayout neo4jLayout;

    private LocationManager locationManager;
    private final String backupName = "neo4j";

    @BeforeEach
    void setUp()
    {
        var managementService = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() )
                .setConfig( online_backup_enabled, true )
                .build();

        var database = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        var db = database.databaseLayout();
        neo4jLayout = db.getNeo4jLayout();

        var neo4jLayoutFolder = db.databaseDirectory().getParent().getParent().getParent();
        neo4jLayout = Neo4jLayout.of( neo4jLayoutFolder );
        managementService.shutdown();

        locationManager = locationManager( backupName );
    }

    @Test
    void shouldGiveAnEmptyLocationIfDestinationIsEmpty() throws IOException
    {
        var backupLocation = locationManager( "dontExist" ).createBackupLocation();
        assertThat( backupLocation ).matches( bl -> !bl.hasExistingStore() );
    }

    @Test
    void shouldGiveAnExistingStoreIfThereIs() throws IOException
    {
        var databaseLayout = neo4jLayout.databaseLayout( backupName );
        var expectedStoreId = new StoreFiles( testDirectory.getFileSystem(), pageCache ).readStoreId( databaseLayout, PageCursorTracer.NULL );

        var backupLocation = locationManager.createBackupLocation();

        assertThat( backupLocation ).matches( BackupLocation::hasExistingStore, "Has an existing store" )
                                    .matches( bl -> bl.databaseId().isEmpty(), "Does not have a database id" );

        assertThat( backupLocation.storeId() ).contains( expectedStoreId );
    }

    @Test
    void shouldFallBackWhenFailed() throws IOException
    {
        var firstLocation = locationManager.createBackupLocation();

        // during work dirs name contains ongoing
        var firstDir = firstLocation.databaseLayout().databaseDirectory();
        assertThat( firstDir ).isEqualByComparingTo( neo4jLayout.databaseLayout( backupName ).databaseDirectory() );
        var fallBackLocation = locationManager.createTemporaryEmptyLocation();
        // fallback location is the same but is empty...

        var tmpDir = neo4jLayout.databasesDirectory().resolve( format( FileManager.WORKING_DIR_PATTERN, backupName, 0 ) );
        assertThat( fallBackLocation.databaseDirectory() ).isEqualByComparingTo( tmpDir ).isEmptyDirectory();

        var fs = testDirectory.getFileSystem();
        fs.copyRecursively( firstDir, tmpDir );

        locationManager.reportSuccessful( fallBackLocation, DatabaseIdFactory.from( UUID.randomUUID() ) );

        // old content is moved to error dir
        var errorDir = neo4jLayout.databasesDirectory().resolve( format( FileManager.ERROR_DIR_PATTERN, backupName, 0 ) );
        assertThat( errorDir ).doesNotExist();
        assertThat( firstLocation ).matches( BackupLocation::hasExistingStore, "has existing store" );
        assertThat( fallBackLocation.storeId() ).isEmpty();

        assertThat( fallBackLocation.databaseDirectory() ).doesNotExist();
        assertThat( neo4jLayout.databaseLayout( backupName ).databaseDirectory() ).isNotEmptyDirectory();
    }

    @Test
    void backupExistingDirectoryWhenNewBackupFails() throws IOException
    {
        var firstLocation = locationManager.createBackupLocation();

        // during work dirs name contains ongoing
        var firstDir = firstLocation.databaseDirectory();
        var outputDirLayout = neo4jLayout.databaseLayout( backupName );
        assertThat( firstDir ).isEqualByComparingTo( outputDirLayout.databaseDirectory() );
        var fallBackLocation = locationManager.createTemporaryEmptyLocation();
        // fallback location is the same but is empty...

        var tmpDir = neo4jLayout.databasesDirectory().resolve( format( FileManager.WORKING_DIR_PATTERN, backupName, 0 ) );
        assertThat( fallBackLocation.databaseDirectory() ).isEqualByComparingTo( tmpDir ).isEmptyDirectory();

        // delete the directory
        var fs = testDirectory.getFileSystem();
        fs.delete( firstLocation.databaseLayout().metadataStore() );

        locationManager.reportSuccessful( fallBackLocation, DatabaseIdFactory.from( UUID.randomUUID() ) );

        // old content is moved to error dir
        var errorDir = neo4jLayout.databasesDirectory().resolve( format( FileManager.ERROR_DIR_PATTERN, backupName, 0 ) );
        assertThat( errorDir ).exists();

        assertThat( fallBackLocation.databaseDirectory() ).doesNotExist();
    }

    @Test
    void shouldThrowIfCompletingWithDifferentDatabaseId() throws IOException
    {
        var oneId = DatabaseIdFactory.from( UUID.randomUUID() );
        var otherId = DatabaseIdFactory.from( UUID.randomUUID() );

        var backupLocation = locationManager.createBackupLocation();
        backupLocation.writeDatabaseId( oneId );
        var exception = assertThrows( IllegalArgumentException.class, () -> locationManager.reportSuccessful( backupLocation, otherId ) );
        assertThat( exception ).hasMessage( format( "Miss-match in database id. Existing backup has %s, trying to write %s", oneId, otherId ) );
    }

    @Test
    void shouldWriteDatabaseIdOnComplete() throws IOException
    {
        var databaseId = DatabaseIdFactory.from( UUID.randomUUID() );
        var backupLocation = locationManager.createBackupLocation();
        assertThat( backupLocation.databaseId() ).isEmpty();
        locationManager.reportSuccessful( backupLocation, databaseId );
        var again = locationManager.createBackupLocation();
        assertThat( again.databaseId() ).contains( databaseId );
    }

    private LocationManager locationManager( String dbName )
    {
        return new LocationManager( testDirectory.getFileSystem(), pageCache, neo4jLayout.databasesDirectory(), dbName, PageCacheTracer.NULL,
                                    nullLogProvider() );
    }
}
