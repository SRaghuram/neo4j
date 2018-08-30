/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.File;

import org.neo4j.causalclustering.catchup.CatchUpClient;
import org.neo4j.causalclustering.catchup.CatchUpClientException;
import org.neo4j.causalclustering.identity.StoreId;
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
import org.neo4j.scheduler.ThreadPoolJobScheduler;

class SimpleCatchupClient implements AutoCloseable
{
    private final GraphDatabaseAPI graphDb;
    private final FileSystemAbstraction fsa;
    private final CatchUpClient catchUpClient;
    private final TestCatchupServer catchupServer;

    private final AdvertisedSocketAddress from;
    private final StoreId correctStoreId;
    private final StreamToDiskProvider streamToDiskProvider;
    private final PageCache clientPageCache;
    private final JobScheduler jobScheduler;
    private final Log log;
    private final LogProvider logProvider;

    SimpleCatchupClient( GraphDatabaseAPI graphDb, FileSystemAbstraction fileSystemAbstraction, CatchUpClient catchUpClient,
            TestCatchupServer catchupServer, File temporaryDirectory, LogProvider logProvider )
    {
        this.graphDb = graphDb;
        this.fsa = fileSystemAbstraction;
        this.catchUpClient = catchUpClient;
        this.catchupServer = catchupServer;

        from = getCatchupServerAddress();
        correctStoreId = getStoreIdFromKernelStoreId( graphDb );
        jobScheduler = new ThreadPoolJobScheduler();
        clientPageCache = createPageCache();
        streamToDiskProvider = new StreamToDiskProvider( temporaryDirectory, fsa, new Monitors() );
        log = logProvider.getLog( SimpleCatchupClient.class );
        this.logProvider = logProvider;
    }

    private PageCache createPageCache()
    {
        return StandalonePageCacheFactory.createPageCache( fsa, jobScheduler );
    }

    PrepareStoreCopyResponse requestListOfFilesFromServer() throws CatchUpClientException
    {
        return requestListOfFilesFromServer( correctStoreId );
    }

    PrepareStoreCopyResponse requestListOfFilesFromServer( StoreId expectedStoreId ) throws CatchUpClientException
    {
        return catchUpClient.makeBlockingRequest( from, new PrepareStoreCopyRequest( expectedStoreId ),
                StoreCopyResponseAdaptors.prepareStoreCopyAdaptor( streamToDiskProvider, logProvider.getLog( SimpleCatchupClient.class ) ) );
    }

    StoreCopyFinishedResponse requestIndividualFile( File file ) throws CatchUpClientException
    {
        return requestIndividualFile( file, correctStoreId );
    }

    StoreCopyFinishedResponse requestIndividualFile( File file, StoreId expectedStoreId ) throws CatchUpClientException
    {
        long lastTransactionId = getCheckPointer( graphDb ).lastCheckPointedTransactionId();
        GetStoreFileRequest storeFileRequest = new GetStoreFileRequest( expectedStoreId, file, lastTransactionId );
        return catchUpClient.makeBlockingRequest( from, storeFileRequest, StoreCopyResponseAdaptors.filesCopyAdaptor( streamToDiskProvider, log ) );
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

    StoreCopyFinishedResponse requestIndexSnapshot( long indexId ) throws CatchUpClientException
    {
        long lastCheckPointedTransactionId = getCheckPointer( graphDb ).lastCheckPointedTransactionId();
        StoreId storeId = getStoreIdFromKernelStoreId( graphDb );
        GetIndexFilesRequest request = new GetIndexFilesRequest( storeId, indexId, lastCheckPointedTransactionId );
        return catchUpClient.makeBlockingRequest( from, request, StoreCopyResponseAdaptors.filesCopyAdaptor( streamToDiskProvider, log ) );
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
}
