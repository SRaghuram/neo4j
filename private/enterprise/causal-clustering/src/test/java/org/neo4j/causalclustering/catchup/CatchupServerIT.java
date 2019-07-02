/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import org.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import org.neo4j.causalclustering.helpers.CausalClusteringTestHelpers;
import org.neo4j.causalclustering.identity.StoreId;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.state.NeoStoreFileListing;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StoreFileMetadata;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.io.fs.FileUtils.relativePath;

// TODO - actually update tests for potential database name related issues.
public class CatchupServerIT
{
    private static final String EXISTING_FILE_NAME = "neostore.nodestore.db";
    private static final StoreId WRONG_STORE_ID = new StoreId( 123, 221, 3131, 45678 );
    private static final LogProvider LOG_PROVIDER = NullLogProvider.getInstance();

    private static final String PROP_NAME = "name";
    private static final String PROP = "prop";
    public static final Label LABEL = label( "MyLabel" );
    private static final String INDEX = "index";

    private GraphDatabaseAPI graphDb;
    private TestCatchupServer catchupServer;
    private File temporaryDirectory;

    private PageCache pageCache;

    @Rule
    public DefaultFileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory( fileSystemRule );
    private CatchupClientFactory catchupClient;
    private DefaultFileSystemAbstraction fsa = fileSystemRule.get();

    @Before
    public void startDb() throws Throwable
    {
        temporaryDirectory = testDirectory.directory( "temp" );
        graphDb = (GraphDatabaseAPI) new TestGraphDatabaseFactory().setFileSystem( fsa ).newEmbeddedDatabase( testDirectory.databaseDir() );
        createLegacyIndex();
        createPropertyIndex();
        addData( graphDb );

        catchupServer = new TestCatchupServer( fsa, graphDb, LOG_PROVIDER );
        catchupServer.start();
        catchupClient = CausalClusteringTestHelpers.getCatchupClient( LOG_PROVIDER );
        catchupClient.start();
        pageCache = graphDb.getDependencyResolver().resolveDependency( PageCache.class );
    }

    @After
    public void stopDb() throws Throwable
    {
        pageCache.flushAndForce();
        if ( graphDb != null )
        {
            graphDb.shutdown();
        }
        if ( catchupClient != null )
        {
            catchupClient.stop();
        }
        if ( catchupServer != null )
        {
            catchupServer.stop();
        }
    }

    @Test
    public void shouldListExpectedFilesCorrectly() throws Exception
    {
        // given (setup) required runtime subject dependencies
        NeoStoreDataSource neoStoreDataSource = getNeoStoreDataSource( graphDb );
        SimpleCatchupClient simpleCatchupClient = new SimpleCatchupClient( graphDb, DEFAULT_DATABASE_NAME, fsa, catchupClient,
                catchupServer, temporaryDirectory, LOG_PROVIDER );

        // when
        PrepareStoreCopyResponse prepareStoreCopyResponse = simpleCatchupClient.requestListOfFilesFromServer();
        simpleCatchupClient.close();

        // then
        listOfDownloadedFilesMatchesServer( neoStoreDataSource, prepareStoreCopyResponse.getFiles() );

        // and downloaded files are identical to source
        List<File> expectedCountStoreFiles = listServerExpectedNonReplayableFiles( neoStoreDataSource );
        for ( File storeFileSnapshot : expectedCountStoreFiles )
        {
            fileContentEquals( databaseFileToClientFile( storeFileSnapshot ), storeFileSnapshot );
        }

        // and
        assertTransactionIdMatches( prepareStoreCopyResponse.lastTransactionId() );

        //and
        assertTrue( "Expected an empty set of ids. Found size " + prepareStoreCopyResponse.getIndexIds().size(),
                prepareStoreCopyResponse.getIndexIds().isEmpty() );
    }

    @Test
    public void shouldCommunicateErrorIfStoreIdDoesNotMatchRequest() throws Exception
    {
        // given (setup) required runtime subject dependencies
        addData( graphDb );
        SimpleCatchupClient simpleCatchupClient = new SimpleCatchupClient( graphDb, DEFAULT_DATABASE_NAME, fsa, catchupClient,
                catchupServer, temporaryDirectory, LOG_PROVIDER );

        // when the list of files are requested from the server with the wrong storeId
        PrepareStoreCopyResponse prepareStoreCopyResponse = simpleCatchupClient.requestListOfFilesFromServer( WRONG_STORE_ID, DEFAULT_DATABASE_NAME );
        simpleCatchupClient.close();

        // then the response is not a list of files but an error
        assertEquals( PrepareStoreCopyResponse.Status.E_STORE_ID_MISMATCH, prepareStoreCopyResponse.status() );

        // and the list of files is empty because the request should have failed
        File[] remoteFiles = prepareStoreCopyResponse.getFiles();
        assertArrayEquals( new File[]{}, remoteFiles );
    }

    @Test
    public void individualFileCopyWorks() throws Exception
    {
        // given a file exists on the server
        addData( graphDb );
        File existingFile = new File( temporaryDirectory, EXISTING_FILE_NAME );

        // and
        SimpleCatchupClient simpleCatchupClient = new SimpleCatchupClient( graphDb, DEFAULT_DATABASE_NAME, fsa, catchupClient,
                catchupServer, temporaryDirectory, LOG_PROVIDER );

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
    public void individualIndexSnapshotCopyWorks() throws Exception
    {

        // given
        NeoStoreDataSource neoStoreDataSource = getNeoStoreDataSource( graphDb );
        List<File> expectingFiles = neoStoreDataSource.getNeoStoreFileListing().builder().excludeAll().includeSchemaIndexStoreFiles().build().stream().map(
                StoreFileMetadata::file ).collect( toList() );
        SimpleCatchupClient simpleCatchupClient = new SimpleCatchupClient( graphDb, DEFAULT_DATABASE_NAME, fsa, catchupClient,
                catchupServer, temporaryDirectory, LOG_PROVIDER );

        // and
        LongIterator indexIds = getExpectedIndexIds( neoStoreDataSource ).longIterator();

        // when
        while ( indexIds.hasNext() )
        {
            long indexId = indexIds.next();
            StoreCopyFinishedResponse response = simpleCatchupClient.requestIndexSnapshot( indexId );
            simpleCatchupClient.close();
            assertEquals( StoreCopyFinishedResponse.Status.SUCCESS, response.status() );
        }

        // then
        fileContentEquals( expectingFiles );
    }

    @Test
    public void individualFileCopyFailsIfStoreIdMismatch() throws Exception
    {
        // given a file exists on the server
        addData( graphDb );
        File expectedExistingFile = graphDb.databaseLayout().file( EXISTING_FILE_NAME );

        // and
        SimpleCatchupClient simpleCatchupClient = new SimpleCatchupClient( graphDb, DEFAULT_DATABASE_NAME, fsa, catchupClient,
                catchupServer, temporaryDirectory, LOG_PROVIDER );

        // when we copy that file using a different storeId
        StoreCopyFinishedResponse storeCopyFinishedResponse =
                simpleCatchupClient.requestIndividualFile( expectedExistingFile, WRONG_STORE_ID, DEFAULT_DATABASE_NAME );
        simpleCatchupClient.close();

        // then the response from the server should be an error message that describes a store ID mismatch
        assertEquals( StoreCopyFinishedResponse.Status.E_STORE_ID_MISMATCH, storeCopyFinishedResponse.status() );
    }

    private void assertTransactionIdMatches( long lastTxId )
    {
        long expectedTransactionId = getCheckPointer( graphDb ).lastCheckPointedTransactionId();
        assertEquals( expectedTransactionId, lastTxId);
    }

    private void fileContentEquals( Collection<File> countStore ) throws IOException
    {
        for ( File file : countStore )
        {
            fileContentEquals( databaseFileToClientFile( file ), file );
        }
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
        assertEquals( message, CausalClusteringTestHelpers.fileContent( fileA, fsa ),
                CausalClusteringTestHelpers.fileContent( fileB, fsa ) );
    }

    private void listOfDownloadedFilesMatchesServer( NeoStoreDataSource neoStoreDataSource, File[] files )
            throws IOException
    {
        List<String> expectedStoreFiles = getExpectedStoreFiles( neoStoreDataSource );
        List<String> givenFile = Arrays.stream( files ).map( File::getName ).collect( toList() );
        assertThat( givenFile, containsInAnyOrder( expectedStoreFiles.toArray( new String[givenFile.size()] ) ) );
    }

    private static LongSet getExpectedIndexIds( NeoStoreDataSource neoStoreDataSource )
    {
        return neoStoreDataSource.getNeoStoreFileListing().getNeoStoreFileIndexListing().getIndexIds();
    }

    private static List<File> listServerExpectedNonReplayableFiles( NeoStoreDataSource neoStoreDataSource ) throws IOException
    {
        try ( Stream<StoreFileMetadata> countStoreStream = neoStoreDataSource.getNeoStoreFileListing().builder().excludeAll()
                .includeNeoStoreFiles().build().stream();
                Stream<StoreFileMetadata> explicitIndexStream = neoStoreDataSource.getNeoStoreFileListing().builder().excludeAll()
                         .includeExplicitIndexStoreStoreFiles().build().stream() )
        {
            return Stream.concat( countStoreStream.filter( isCountFile( neoStoreDataSource.getDatabaseLayout() ) ), explicitIndexStream ).map(
                    StoreFileMetadata::file ).collect( toList() );
        }
    }

    private List<String> getExpectedStoreFiles( NeoStoreDataSource neoStoreDataSource ) throws IOException
    {
        NeoStoreFileListing.StoreFileListingBuilder builder = neoStoreDataSource.getNeoStoreFileListing().builder();
        builder.excludeLogFiles().excludeExplicitIndexStoreFiles().excludeSchemaIndexStoreFiles().excludeLabelScanStoreFiles().excludeAdditionalProviders();
        try ( Stream<StoreFileMetadata> stream = builder.build().stream() )
        {
            return stream.filter( isCountFile( neoStoreDataSource.getDatabaseLayout() ).negate() ).map( sfm -> sfm.file().getName() ).collect( toList() );
        }
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
            tx.success();
        }
    }

    private void createPropertyIndex()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            graphDb.schema().indexFor( LABEL ).on( PROP_NAME ).create();
            tx.success();
        }
    }

    private void createLegacyIndex()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            Index<Node> nodeIndex = graphDb.index().forNodes( INDEX );
            nodeIndex.add( graphDb.createNode(), "some-key", "som-value" );
            tx.success();
        }
    }

    private static CheckPointer getCheckPointer( GraphDatabaseAPI graphDb )
    {
        return graphDb.getDependencyResolver().resolveDependency( CheckPointer.class );
    }

    private static NeoStoreDataSource getNeoStoreDataSource( GraphDatabaseAPI graphDb )
    {
        return graphDb.getDependencyResolver().resolveDependency( NeoStoreDataSource.class );
    }
}
