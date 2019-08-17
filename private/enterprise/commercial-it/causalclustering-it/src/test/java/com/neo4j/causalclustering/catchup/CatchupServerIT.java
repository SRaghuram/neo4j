/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import com.neo4j.causalclustering.common.CausalClusteringTestHelpers;
import com.neo4j.test.TestCommercialDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseFile;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.availability.DatabaseAvailabilityGuard;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.state.DatabaseFileListing;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.DefaultFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse.Status;
import static com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.E_DATABASE_UNKNOWN;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCauseMessage;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.io.fs.FileUtils.relativePath;

@ExtendWith( {DefaultFileSystemExtension.class, TestDirectoryExtension.class} )
class CatchupServerIT
{
    private static final String EXISTING_FILE_NAME = DatabaseFile.NODE_STORE.getName();
    private static final DatabaseId UNKNOWN_DB_ID = new TestDatabaseIdRepository().get( "unknown.db" );
    private static final StoreId WRONG_STORE_ID = new StoreId( 123, 221, 1122, 3131, 45678 );
    private static final LogProvider LOG_PROVIDER = NullLogProvider.getInstance();

    private static final String PROP_NAME = "name";
    private static final String PROP = "prop";
    private static final Label LABEL = label( "MyLabel" );

    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    private GraphDatabaseAPI db;
    private TestCatchupServer catchupServer;
    private File temporaryDirectory;
    private ExecutorService executor;
    private PageCache pageCache;
    private CatchupClientFactory catchupClient;
    private DatabaseManagementService managementService;
    private DatabaseIdRepository databaseIdRepository;

    @BeforeEach
    void startDb()
    {
        temporaryDirectory = testDirectory.directory( "temp" );
        managementService = new TestCommercialDatabaseManagementServiceBuilder( testDirectory.storeDir() )
                .setFileSystem( fs )
                .setConfig( auth_enabled, true )
                .build();
        db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        createPropertyIndex();
        addData( db );

        DatabaseManager<?> databaseManager = db.getDependencyResolver().resolveDependency( DatabaseManager.class );
        databaseIdRepository = databaseManager.databaseIdRepository();
        MultiDatabaseCatchupServerHandler catchupServerHandler = new MultiDatabaseCatchupServerHandler( databaseManager, fs,
                LOG_PROVIDER );

        executor = Executors.newCachedThreadPool();
        catchupServer = new TestCatchupServer( catchupServerHandler, LOG_PROVIDER, executor );
        catchupServer.start();
        catchupClient = CausalClusteringTestHelpers.getCatchupClient( LOG_PROVIDER, new ThreadPoolJobScheduler( executor ) );
        catchupClient.start();
        pageCache = db.getDependencyResolver().resolveDependency( PageCache.class );
    }

    @AfterEach
    void stopDb() throws Throwable
    {
        pageCache.flushAndForce();
        if ( db != null )
        {
            managementService.shutdown();
        }
        if ( catchupClient != null )
        {
            catchupClient.stop();
        }
        if ( catchupServer != null )
        {
            catchupServer.stop();
        }
        executor.shutdown();
    }

    @Test
    void shouldListExpectedFilesCorrectly() throws Exception
    {
        // given (setup) required runtime subject dependencies
        Database database = getDatabase( db );
        SimpleCatchupClient simpleCatchupClient = newSimpleCatchupClient();

        // when
        PrepareStoreCopyResponse prepareStoreCopyResponse = simpleCatchupClient.requestListOfFilesFromServer();
        simpleCatchupClient.close();

        // then
        listOfDownloadedFilesMatchesServer( database, prepareStoreCopyResponse.getFiles() );

        // and downloaded files are identical to source
        List<File> expectedCountStoreFiles = listServerExpectedNonReplayableFiles( database );
        for ( File storeFileSnapshot : expectedCountStoreFiles )
        {
            fileContentEquals( databaseFileToClientFile( storeFileSnapshot ), storeFileSnapshot );
        }

        // and
        assertTransactionIdMatches( prepareStoreCopyResponse.lastCheckPointedTransactionId() );
    }

    @Test
    void shouldCommunicateErrorIfStoreIdDoesNotMatchRequest() throws Exception
    {
        // given (setup) required runtime subject dependencies
        addData( db );
        SimpleCatchupClient simpleCatchupClient = newSimpleCatchupClient();

        // when the list of files are requested from the server with the wrong storeId
        PrepareStoreCopyResponse prepareStoreCopyResponse =
                simpleCatchupClient.requestListOfFilesFromServer( WRONG_STORE_ID, databaseIdRepository.get( DEFAULT_DATABASE_NAME ) );
        simpleCatchupClient.close();

        // then the response is not a list of files but an error
        assertEquals( Status.E_STORE_ID_MISMATCH, prepareStoreCopyResponse.status() );

        // and the list of files is empty because the request should have failed
        File[] remoteFiles = prepareStoreCopyResponse.getFiles();
        assertArrayEquals( new File[]{}, remoteFiles );
    }

    @Test
    void individualFileCopyWorks() throws Exception
    {
        // given a file exists on the server
        addData( db );
        File existingFile = new File( temporaryDirectory, EXISTING_FILE_NAME );

        // and
        SimpleCatchupClient simpleCatchupClient = newSimpleCatchupClient();

        // when we copy that file
        pageCache.flushAndForce();
        StoreCopyFinishedResponse storeCopyFinishedResponse = simpleCatchupClient.requestIndividualFile( existingFile );
        simpleCatchupClient.close();

        // then the response is successful
        assertEquals( StoreCopyFinishedResponse.Status.SUCCESS, storeCopyFinishedResponse.status() );

        // then the contents matches
        fileContentEquals( clientFileToDatabaseFile( existingFile ), existingFile );
    }

    @Test
    void individualFileCopyFailsIfStoreIdMismatch() throws Exception
    {
        // given a file exists on the server
        addData( db );
        File expectedExistingFile = db.databaseLayout().file( EXISTING_FILE_NAME );

        // and
        SimpleCatchupClient simpleCatchupClient = newSimpleCatchupClient();

        // when we copy that file using a different storeId
        StoreCopyFinishedResponse storeCopyFinishedResponse =
                simpleCatchupClient.requestIndividualFile( expectedExistingFile, WRONG_STORE_ID, databaseIdRepository.get( DEFAULT_DATABASE_NAME ) );
        simpleCatchupClient.close();

        // then the response from the server should be an error message that describes a store ID mismatch
        assertEquals( StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH, storeCopyFinishedResponse.status() );
    }

    @Test
    void shouldFailWithGoodErrorWhenListingFilesForDatabaseThatDoesNotExist() throws Exception
    {
        try ( SimpleCatchupClient simpleCatchupClient = newSimpleCatchupClient( UNKNOWN_DB_ID ) )
        {
            Exception error = assertThrows( Exception.class, simpleCatchupClient::requestListOfFilesFromServer );
            assertThat( getRootCauseMessage( error ), containsString( UNKNOWN_DB_ID.name() + " does not exist" ) );
        }
    }

    @Test
    void shouldReturnCorrectStatusWhenRequestingFileForDatabaseThatDoesNotExist() throws Exception
    {
        try ( SimpleCatchupClient simpleCatchupClient = newSimpleCatchupClient( UNKNOWN_DB_ID ) )
        {
            // individual file request does not throw when error response is received, it returns a status instead
            StoreCopyFinishedResponse response = simpleCatchupClient.requestIndividualFile( new File( EXISTING_FILE_NAME ) );
            assertEquals( E_DATABASE_UNKNOWN, response.status() );
        }
    }

    @Test
    void shouldFailWhenRequestedDatabaseIsShutdown() throws Exception
    {
        var databaseName = "foo";
        managementService.createDatabase( databaseName );

        try ( var catchupClient = newSimpleCatchupClient( databaseIdRepository.get( databaseName ) ) )
        {
            managementService.shutdownDatabase( databaseName );

            var error = assertThrows( Exception.class, catchupClient::requestListOfFilesFromServer );
            assertThat( getRootCauseMessage( error ), containsString( "database " + databaseName + " is shutdown" ) );
        }
    }

    @Test
    void shouldFailWhenRequestedDatabaseIsUnavailable() throws Exception
    {
        var databaseName = "bar";
        managementService.createDatabase( databaseName );

        try ( var catchupClient = newSimpleCatchupClient( databaseIdRepository.get( databaseName ) ) )
        {
            makeDatabaseUnavailable( databaseName, managementService );

            var error = assertThrows( Exception.class, catchupClient::requestListOfFilesFromServer );
            assertThat( getRootCauseMessage( error ), containsString( "database " + databaseName + " is unavailable" ) );
        }
    }

    private void assertTransactionIdMatches( long lastTxId )
    {
        long expectedTransactionId = getCheckPointer( db ).lastCheckPointedTransactionId();
        assertEquals( expectedTransactionId, lastTxId);
    }

    private File databaseFileToClientFile( File file ) throws IOException
    {
        String relativePathToDatabaseDir = relativePath( testDirectory.databaseDir(), file );
        return new File( temporaryDirectory, relativePathToDatabaseDir );
    }

    private File clientFileToDatabaseFile( File file ) throws IOException
    {
        String relativePathToDatabaseDir = relativePath( temporaryDirectory, file );
        return new File( testDirectory.databaseDir(), relativePathToDatabaseDir );
    }

    private void fileContentEquals( File fileA, File fileB ) throws IOException
    {
        assertNotEquals( fileA.getPath(), fileB.getPath() );
        String message = String.format( "Expected file: %s\ndoes not match actual file: %s", fileA, fileB );
        assertEquals( CausalClusteringTestHelpers.fileContent( fileA, fs ), CausalClusteringTestHelpers.fileContent( fileB, fs ), message );
    }

    private void listOfDownloadedFilesMatchesServer( Database database, File[] files )
            throws IOException
    {
        List<String> expectedStoreFiles = getExpectedStoreFiles( database );
        List<String> givenFile = Arrays.stream( files ).map( File::getName ).collect( toList() );
        assertThat( givenFile, containsInAnyOrder( expectedStoreFiles.toArray( new String[givenFile.size()] ) ) );
    }

    private static List<File> listServerExpectedNonReplayableFiles( Database database ) throws IOException
    {
        try ( Stream<StoreFileMetadata> countStoreStream = database.getDatabaseFileListing().builder().excludeAll()
                .includeNeoStoreFiles().build().stream();
                Stream<StoreFileMetadata> explicitIndexStream = database.getDatabaseFileListing().builder().excludeAll()
                         .build().stream() )
        {
            return Stream.concat( countStoreStream.filter( isCountFile( database.getDatabaseLayout() ) ), explicitIndexStream ).map(
                    StoreFileMetadata::file ).collect( toList() );
        }
    }

    private List<String> getExpectedStoreFiles( Database database ) throws IOException
    {
        DatabaseFileListing.StoreFileListingBuilder builder = database.getDatabaseFileListing().builder();
        builder.excludeLogFiles().excludeSchemaIndexStoreFiles().excludeLabelScanStoreFiles().excludeAdditionalProviders();
        try ( Stream<StoreFileMetadata> stream = builder.build().stream() )
        {
            return stream.filter( isCountFile( database.getDatabaseLayout() ).negate() ).map( sfm -> sfm.file().getName() ).collect( toList() );
        }
    }

    private SimpleCatchupClient newSimpleCatchupClient()
    {
        return newSimpleCatchupClient( databaseIdRepository.get( DEFAULT_DATABASE_NAME ) );
    }

    private SimpleCatchupClient newSimpleCatchupClient( DatabaseId databaseId )
    {
        return new SimpleCatchupClient( db, databaseId, fs, catchupClient, catchupServer, temporaryDirectory, LOG_PROVIDER );
    }

    private static Predicate<StoreFileMetadata> isCountFile( DatabaseLayout databaseLayout )
    {
        return storeFileMetadata -> databaseLayout.countStoreA().equals( storeFileMetadata.file() ) ||
                databaseLayout.countStoreB().equals( storeFileMetadata.file() );
    }

    private static void addData( GraphDatabaseAPI graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            Node node = graphDb.createNode();
            node.addLabel( LABEL );
            node.setProperty( PROP_NAME, "Neo" );
            node.setProperty( PROP, Math.random() * 10000 );
            graphDb.createNode().createRelationshipTo( node, RelationshipType.withName( "KNOWS" ) );
            tx.commit();
        }
    }

    private void createPropertyIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().indexFor( LABEL ).on( PROP_NAME ).create();
            tx.commit();
        }
    }

    private static CheckPointer getCheckPointer( GraphDatabaseAPI graphDb )
    {
        return graphDb.getDependencyResolver().resolveDependency( CheckPointer.class );
    }

    private static Database getDatabase( GraphDatabaseAPI graphDb )
    {
        return graphDb.getDependencyResolver().resolveDependency( Database.class );
    }

    private static void makeDatabaseUnavailable( String databaseName, DatabaseManagementService managementService )
    {
        var db = (GraphDatabaseAPI) managementService.database( databaseName );
        var availabilityGuard = db.getDependencyResolver().resolveDependency( DatabaseAvailabilityGuard.class );
        availabilityGuard.require( () -> "Unavailable for testing" );
    }
}
