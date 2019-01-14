/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import org.eclipse.collections.api.set.primitive.LongSet;

import java.io.File;
import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.CatchupServerProtocol;
import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.NeoStoreDataSource;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;

import static org.neo4j.causalclustering.catchup.storecopy.DataSourceChecks.hasSameStoreId;

public class PrepareStoreCopyRequestHandler extends SimpleChannelInboundHandler<PrepareStoreCopyRequest>
{
    private final CatchupServerProtocol protocol;
    private final PrepareStoreCopyFilesProvider prepareStoreCopyFilesProvider;
    private final Supplier<NeoStoreDataSource> dataSourceSupplier;
    private final StoreFileStreamingProtocol streamingProtocol = new StoreFileStreamingProtocol();

    public PrepareStoreCopyRequestHandler( CatchupServerProtocol catchupServerProtocol, Supplier<NeoStoreDataSource> dataSourceSupplier,
            PrepareStoreCopyFilesProvider prepareStoreCopyFilesProvider )
    {
        this.protocol = catchupServerProtocol;
        this.prepareStoreCopyFilesProvider = prepareStoreCopyFilesProvider;
        this.dataSourceSupplier = dataSourceSupplier;
    }

    @Override
    protected void channelRead0( ChannelHandlerContext channelHandlerContext, PrepareStoreCopyRequest prepareStoreCopyRequest ) throws IOException
    {
        CloseablesListener closeablesListener = new CloseablesListener();
        PrepareStoreCopyResponse response = PrepareStoreCopyResponse.error( PrepareStoreCopyResponse.Status.E_LISTING_STORE );
        try
        {
            NeoStoreDataSource neoStoreDataSource = dataSourceSupplier.get();
            if ( !hasSameStoreId( prepareStoreCopyRequest.getStoreId(), neoStoreDataSource ) )
            {
                channelHandlerContext.write( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE );
                response = PrepareStoreCopyResponse.error( PrepareStoreCopyResponse.Status.E_STORE_ID_MISMATCH );
            }
            else
            {
                CheckPointer checkPointer = neoStoreDataSource.getDependencyResolver().resolveDependency( CheckPointer.class );
                closeablesListener.add( tryCheckpointAndAcquireMutex( checkPointer ) );
                PrepareStoreCopyFiles prepareStoreCopyFiles =
                        closeablesListener.add( prepareStoreCopyFilesProvider.prepareStoreCopyFiles( neoStoreDataSource ) );

                StoreResource[] nonReplayable = prepareStoreCopyFiles.getAtomicFilesSnapshot();
                for ( StoreResource storeResource : nonReplayable )
                {
                    streamingProtocol.stream( channelHandlerContext, storeResource );
                }
                channelHandlerContext.write( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE );
                response = createSuccessfulResponse( checkPointer, prepareStoreCopyFiles );
            }
        }
        finally
        {
            channelHandlerContext.writeAndFlush( response ).addListener( closeablesListener );
            protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
        }
    }

    private PrepareStoreCopyResponse createSuccessfulResponse( CheckPointer checkPointer, PrepareStoreCopyFiles prepareStoreCopyFiles ) throws IOException
    {
        LongSet indexIds = prepareStoreCopyFiles.getNonAtomicIndexIds();
        File[] files = prepareStoreCopyFiles.listReplayableFiles();
        long lastCommittedTxId = checkPointer.lastCheckPointedTransactionId();
        return PrepareStoreCopyResponse.success( files, indexIds, lastCommittedTxId );
    }

    private Resource tryCheckpointAndAcquireMutex( CheckPointer checkPointer ) throws IOException
    {
        return dataSourceSupplier.get().getStoreCopyCheckPointMutex().storeCopy( () -> checkPointer.tryCheckPoint( new SimpleTriggerInfo( "Store copy" ) ) );
    }
}
