/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.storecopy.PrepareStoreCopyResponse;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyFinishedResponse;
import com.neo4j.causalclustering.catchup.storecopy.StoreCopyResponseAdaptors;
import com.neo4j.causalclustering.catchup.storecopy.StreamToDiskProvider;
import com.neo4j.causalclustering.identity.StoreId;

import java.io.File;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

/**
 * For use in ITs, for mocking purposes see {@link MockCatchupClient}
 */
class SimpleCatchupClient implements AutoCloseable
{
    private final GraphDatabaseAPI graphDb;
    private final FileSystemAbstraction fsa;
    private final CatchupClientFactory catchUpClientFactory;
    private final TestCatchupServer catchupServer;
    private final String correctDatabaseName;

    private final AdvertisedSocketAddress from;
    private final StoreId correctStoreId;
    private final StreamToDiskProvider streamToDiskProvider;
    private final PageCache clientPageCache;
    private final JobScheduler jobScheduler;
    private final Log log;
    private final LogProvider logProvider;

    SimpleCatchupClient( GraphDatabaseAPI graphDb, String databaseName, FileSystemAbstraction fileSystemAbstraction,
            CatchupClientFactory catchUpClientFactory, TestCatchupServer catchupServer, File temporaryDirectory, LogProvider logProvider )
    {
        this.graphDb = graphDb;
        this.fsa = fileSystemAbstraction;
        this.catchUpClientFactory = catchUpClientFactory;
        this.catchupServer = catchupServer;
        this.correctDatabaseName = databaseName;

        from = getCatchupServerAddress();
        correctStoreId = getStoreIdFromKernelStoreId( graphDb );
        jobScheduler = new ThreadPoolJobScheduler();
        clientPageCache = createPageCache();
        streamToDiskProvider = new StreamToDiskProvider( temporaryDirectory, fsa, new Monitors() );
        log = logProvider.getLog( SimpleCatchupClient.class );
        this.logProvider = logProvider;
    }

    public PrepareStoreCopyResponse requestListOfFilesFromServer() throws Exception
    {
        return requestListOfFilesFromServer( correctStoreId, correctDatabaseName );
    }

    public PrepareStoreCopyResponse requestListOfFilesFromServer( StoreId expectedStoreId, String expectedDatabaseName ) throws Exception
    {
        CatchupResponseAdaptor<PrepareStoreCopyResponse> responseHandler =
                StoreCopyResponseAdaptors.prepareStoreCopyAdaptor( streamToDiskProvider, logProvider.getLog( SimpleCatchupClient.class ) );
        return catchUpClientFactory.getClient( from, log )
                .v1( c -> c.prepareStoreCopy( expectedStoreId ) )
                .v2( c -> c.prepareStoreCopy( expectedStoreId, expectedDatabaseName ) ).v3( c -> c.prepareStoreCopy( expectedStoreId, expectedDatabaseName ) )
                .withResponseHandler( responseHandler )
                .request();
    }

    public StoreCopyFinishedResponse requestIndividualFile( File file ) throws Exception
    {
        return requestIndividualFile( file, correctStoreId, correctDatabaseName );
    }

    public StoreCopyFinishedResponse requestIndividualFile( File file, StoreId expectedStoreId, String expectedDatabaseName ) throws Exception
    {
        long lastTransactionId = getCheckPointer( graphDb ).lastCheckPointedTransactionId();
        CatchupResponseAdaptor<StoreCopyFinishedResponse> responseHandler = StoreCopyResponseAdaptors.filesCopyAdaptor( streamToDiskProvider, log );
        return catchUpClientFactory.getClient( from, log )
                .v1( c -> c.getStoreFile( expectedStoreId, file, lastTransactionId ) )
                .v2( c -> c.getStoreFile( expectedStoreId, file, lastTransactionId, expectedDatabaseName ) )
                .v3( c -> c.getStoreFile( expectedStoreId, file, lastTransactionId, expectedDatabaseName ) )
                .withResponseHandler( responseHandler )
                .request();
    }

    public StoreCopyFinishedResponse requestIndexSnapshot( long indexId ) throws Exception
    {
        long lastCheckPointedTransactionId = getCheckPointer( graphDb ).lastCheckPointedTransactionId();
        StoreId storeId = getStoreIdFromKernelStoreId( graphDb );
        CatchupResponseAdaptor<StoreCopyFinishedResponse> responseHandler = StoreCopyResponseAdaptors.filesCopyAdaptor( streamToDiskProvider, log );

        return catchUpClientFactory.getClient( from, log )
                .v1( c -> c.getIndexFiles( storeId, indexId, lastCheckPointedTransactionId ) )
                .v2( c -> c.getIndexFiles( storeId, indexId, lastCheckPointedTransactionId, correctDatabaseName ) )
                .v3( c -> c.getIndexFiles( storeId, indexId, lastCheckPointedTransactionId, correctDatabaseName ) )
                .withResponseHandler( responseHandler )
                .request();
    }

    @Override
    public void close() throws Exception
    {
        IOUtils.closeAll( clientPageCache, jobScheduler );
    }

    private static CheckPointer getCheckPointer( GraphDatabaseAPI graphDb )
    {
        return graphDb.getDependencyResolver().resolveDependency( CheckPointer.class );
    }

    private StoreId getStoreIdFromKernelStoreId( GraphDatabaseAPI graphDb )
    {
        org.neo4j.storageengine.api.StoreId storeId = graphDb.storeId();
        return new StoreId( storeId.getCreationTime(), storeId.getRandomId(), storeId.getUpgradeTime(), storeId.getUpgradeId() );
    }

    private AdvertisedSocketAddress getCatchupServerAddress()
    {
        return new AdvertisedSocketAddress( "localhost", catchupServer.address().getPort() );
    }

    private PageCache createPageCache()
    {
        return StandalonePageCacheFactory.createPageCache( fsa, jobScheduler );
    }
}
