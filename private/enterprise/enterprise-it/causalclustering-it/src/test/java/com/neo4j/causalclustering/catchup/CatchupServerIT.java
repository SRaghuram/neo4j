/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import com.neo4j.causalclustering.catchup.v4.info.InfoProvider;
import com.neo4j.causalclustering.common.CausalClusteringTestHelpers;
import com.neo4j.test.TestEnterpriseDatabaseManagementServiceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.common.DependencyResolver;
import org.neo4j.dbms.DatabaseStateService;
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
import org.neo4j.kernel.database.DatabaseIdRepository;
import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.state.DatabaseFileListing;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

import static com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse.Status;
import static com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse.Status.E_DATABASE_UNKNOWN;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCauseMessage;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.graphdb.Label.label;

@Neo4jLayoutExtension
class CatchupServerIT
{
    private static final String EXISTING_FILE_NAME = DatabaseFile.NODE_STORE.getName();
    private static final NamedDatabaseId UNKNOWN_NAMED_DB_ID = TestDatabaseIdRepository.randomNamedDatabaseId();
    private static final StoreId WRONG_STORE_ID = new StoreId( 123, 221, 1122, 3131, 45678 );
    private static final LogProvider LOG_PROVIDER = NullLogProvider.getInstance();

    private static final String PROP_NAME = "name";
    private static final String PROP = "prop";
    private static final Label LABEL = label( "MyLabel" );

    @Inject
    private FileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;
    @Inject
    private DatabaseLayout databaseLayout;

    private GraphDatabaseAPI db;
    private TestCatchupServer catchupServer;
    private Path temporaryDirectory;
    private ExecutorService executor;
    private PageCache pageCache;
    private CatchupClientFactory catchupClient;
    private DatabaseManagementService managementService;
    private DatabaseIdRepository databaseIdRepository;
    private DatabaseManager<?> databaseManager;
    private DatabaseStateService databaseStateService;

    @BeforeEach
    void startDb()
    {
        temporaryDirectory = testDirectory.directory( "temp" );
        TestEnterpriseDatabaseManagementServiceBuilder builder = new TestEnterpriseDatabaseManagementServiceBuilder( testDirectory.homePath() );
        configure( builder );
        managementService = builder.build();
        db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        createPropertyIndex();
        addData( db );

        databaseManager = db.getDependencyResolver().resolveDependency( DatabaseManager.class );
        databaseIdRepository = databaseManager.databaseIdRepository();
        databaseStateService = db.getDependencyResolver().resolveDependency(
                DatabaseStateService.class );
        final var dependencyResolver = Mockito.mock( DependencyResolver.class );
        MultiDatabaseCatchupServerHandler catchupServerHandler =
                new MultiDatabaseCatchupServerHandler( databaseManager, databaseStateService, fs, 32768, LOG_PROVIDER, dependencyResolver );

        executor = Executors.newCachedThreadPool();
        catchupServer = new TestCatchupServer( catchupServerHandler, LOG_PROVIDER, executor );
        catchupServer.start();
        catchupClient = CausalClusteringTestHelpers.getCatchupClient( LOG_PROVIDER, new ThreadPoolJobScheduler( executor ) );
        catchupClient.start();
        pageCache = db.getDependencyResolver().resolveDependency( PageCache.class );
    }

    void configure( TestEnterpriseDatabaseManagementServiceBuilder builder )
    {
        builder.setFileSystem( fs ).setConfig( auth_enabled, true );
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
        listOfDownloadedFilesMatchesServer( database, prepareStoreCopyResponse.getPaths() );

        // and downloaded files are identical to source
        List<Path> expectedCountStoreFiles = listServerExpectedNonReplayableFiles( database );
        for ( Path storeFileSnapshot : expectedCountStoreFiles )
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
                simpleCatchupClient.requestListOfFilesFromServer( WRONG_STORE_ID, databaseIdRepository.getByName( DEFAULT_DATABASE_NAME ).get() );
        simpleCatchupClient.close();

        // then the response is not a list of files but an error
        assertEquals( Status.E_STORE_ID_MISMATCH, prepareStoreCopyResponse.status() );

        // and the list of files is empty because the request should have failed
        Path[] remoteFiles = prepareStoreCopyResponse.getPaths();
        assertArrayEquals( new Path[]{}, remoteFiles );
    }

    @Test
    void individualFileCopyWorks() throws Exception
    {
        // given a file exists on the server
        addData( db );
        Path existingFile = temporaryDirectory.resolve( EXISTING_FILE_NAME );

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
        Path expectedExistingFile = db.databaseLayout().file( EXISTING_FILE_NAME );

        // and
        SimpleCatchupClient simpleCatchupClient = newSimpleCatchupClient();

        // when we copy that file using a different storeId
        StoreCopyFinishedResponse storeCopyFinishedResponse = simpleCatchupClient.requestIndividualFile( expectedExistingFile, WRONG_STORE_ID,
                databaseIdRepository.getByName( DEFAULT_DATABASE_NAME ).get() );
        simpleCatchupClient.close();

        // then the response from the server should be an error message that describes a store ID mismatch
        assertEquals( StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH, storeCopyFinishedResponse.status() );
    }

    @Test
    void shouldFailWithGoodErrorWhenListingFilesForDatabaseThatDoesNotExist() throws Exception
    {
        try ( SimpleCatchupClient simpleCatchupClient = newSimpleCatchupClient( UNKNOWN_NAMED_DB_ID ) )
        {
            Exception error = assertThrows( Exception.class, simpleCatchupClient::requestListOfFilesFromServer );
            assertThat( getRootCauseMessage( error ),
                    allOf(
                            containsString( UNKNOWN_NAMED_DB_ID.databaseId().toString() ),
                            containsString( "does not exist" ) ) );
        }
    }

    @Test
    void shouldReturnCorrectStatusWhenRequestingFileForDatabaseThatDoesNotExist() throws Exception
    {
        try ( SimpleCatchupClient simpleCatchupClient = newSimpleCatchupClient( UNKNOWN_NAMED_DB_ID ) )
        {
            // individual file request does not throw when error response is received, it returns a status instead
            StoreCopyFinishedResponse response = simpleCatchupClient.requestIndividualFile( Path.of( EXISTING_FILE_NAME ) );
            assertEquals( E_DATABASE_UNKNOWN, response.status() );
        }
    }

    @Test
    void shouldFailWhenRequestedDatabaseIsShutdown() throws Exception
    {
        var databaseName = "foo";
        managementService.createDatabase( databaseName );

        NamedDatabaseId namedDatabaseId = databaseIdRepository.getByName( databaseName ).get();
        try ( var catchupClient = newSimpleCatchupClient( namedDatabaseId ) )
        {
            managementService.shutdownDatabase( databaseName );

            var error = assertThrows( Exception.class, catchupClient::requestListOfFilesFromServer );
            assertThat( getRootCauseMessage( error ),
                    allOf(
                            containsString( "database" ),
                            containsString( namedDatabaseId.databaseId().toString() ),
                            containsString( "is shutdown" ) ) );
        }
    }

    @Test
    void shouldFailWhenRequestedDatabaseIsUnavailable() throws Exception
    {
        var databaseName = "bar";
        managementService.createDatabase( databaseName );

        NamedDatabaseId namedDatabaseId = databaseIdRepository.getByName( databaseName ).get();
        try ( var catchupClient = newSimpleCatchupClient( namedDatabaseId ) )
        {
            makeDatabaseUnavailable( databaseName, managementService );

            var error = assertThrows( Exception.class, catchupClient::requestListOfFilesFromServer );
            assertThat( getRootCauseMessage( error ),
                    allOf(
                            containsString( "database" ),
                            containsString( namedDatabaseId.databaseId().toString() ),
                            containsString( "is unavailable" ) ) );
        }
    }

    @Test
    void shouldProvideExpectedReconciledInfo() throws Exception
    {
        var databaseName = "bar";
        managementService.createDatabase( databaseName );

        NamedDatabaseId namedDatabaseId = databaseIdRepository.getByName( databaseName ).get();
        try ( var catchupClient = newSimpleCatchupClient( namedDatabaseId ) )
        {
            var infoResponse = catchupClient.requestReconcilerInfo( namedDatabaseId );

            var expectedInfo = new InfoProvider( databaseManager, databaseStateService ).getInfo( namedDatabaseId );

            assertEquals( infoResponse, expectedInfo );
        }
    }

    private void assertTransactionIdMatches( long lastTxId )
    {
        long expectedTransactionId = getCheckPointer( db ).lastCheckPointedTransactionId();
        assertEquals( expectedTransactionId, lastTxId );
    }

    private Path databaseFileToClientFile( Path path )
    {
        Path relativePathToDatabaseDir = databaseLayout.databaseDirectory().relativize( path );
        return temporaryDirectory.resolve( relativePathToDatabaseDir );
    }

    private Path clientFileToDatabaseFile( Path path )
    {
        Path relativePathToDatabaseDir = temporaryDirectory.relativize( path );
        return databaseLayout.databaseDirectory().resolve( relativePathToDatabaseDir );
    }

    private void fileContentEquals( Path pathA, Path pathB ) throws IOException
    {
        assertNotEquals( pathA, pathB );
        String message = String.format( "Expected file: %s\ndoes not match actual file: %s", pathA, pathB );
        assertEquals( CausalClusteringTestHelpers.fileContent( pathA, fs ), CausalClusteringTestHelpers.fileContent( pathB, fs ), message );
    }

    private void listOfDownloadedFilesMatchesServer( Database database, Path[] files )
            throws IOException
    {
        List<String> expectedStoreFiles = getExpectedStoreFiles( database );
        List<String> givenFile = Arrays.stream( files ).map( Path::getFileName ).map( Path::toString ).collect( toList() );
        assertThat( givenFile, containsInAnyOrder( expectedStoreFiles.toArray( new String[givenFile.size()] ) ) );
    }

    private static List<Path> listServerExpectedNonReplayableFiles( Database database ) throws IOException
    {
        try ( Stream<StoreFileMetadata> countStoreStream = database.getDatabaseFileListing().builder().excludeAll()
                .includeNeoStoreFiles().build().stream();
                Stream<StoreFileMetadata> explicitIndexStream = database.getDatabaseFileListing().builder().excludeAll()
                         .build().stream() )
        {
            return Stream.concat( countStoreStream.filter( isCountFile( database.getDatabaseLayout() ) ), explicitIndexStream ).map(
                    StoreFileMetadata::path ).collect( toList() );
        }
    }

    private List<String> getExpectedStoreFiles( Database database ) throws IOException
    {
        DatabaseFileListing.StoreFileListingBuilder builder = database.getDatabaseFileListing().builder();
        builder.excludeLogFiles().excludeSchemaIndexStoreFiles().excludeLabelScanStoreFiles().excludeRelationshipTypeScanStoreFiles()
                .excludeAdditionalProviders().excludeIdFiles();
        try ( Stream<StoreFileMetadata> stream = builder.build().stream() )
        {
            return stream.filter( isCountFile( database.getDatabaseLayout() ).negate() ).map( sfm -> sfm.path().getFileName().toString() ).collect( toList() );
        }
    }

    private SimpleCatchupClient newSimpleCatchupClient()
    {
        return newSimpleCatchupClient( databaseIdRepository.getByName( DEFAULT_DATABASE_NAME ).get() );
    }

    private SimpleCatchupClient newSimpleCatchupClient( NamedDatabaseId namedDatabaseId )
    {
        return new SimpleCatchupClient( db, namedDatabaseId, fs, catchupClient, catchupServer, temporaryDirectory, LOG_PROVIDER );
    }

    private static Predicate<StoreFileMetadata> isCountFile( DatabaseLayout databaseLayout )
    {
        return storeFileMetadata -> databaseLayout.countStore().equals( storeFileMetadata.path() );
    }

    private static void addData( GraphDatabaseAPI graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            Node node = tx.createNode();
            node.addLabel( LABEL );
            node.setProperty( PROP_NAME, "Neo" );
            node.setProperty( PROP, Math.random() * 10000 );
            tx.createNode().createRelationshipTo( node, RelationshipType.withName( "KNOWS" ) );
            tx.commit();
        }
    }

    private void createPropertyIndex()
    {
        try ( Transaction tx = db.beginTx() )
        {
            tx.schema().indexFor( LABEL ).on( PROP_NAME ).create();
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
