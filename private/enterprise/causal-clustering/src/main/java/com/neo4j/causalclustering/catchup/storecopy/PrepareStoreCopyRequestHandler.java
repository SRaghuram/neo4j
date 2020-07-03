/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.CatchupServerProtocol;
import com.neo4j.causalclustering.catchup.ResponseMessageType;
import com.neo4j.causalclustering.catchup.v3.storecopy.PrepareStoreCopyRequest;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;

import org.neo4j.graphdb.Resource;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.logging.Log;

public class PrepareStoreCopyRequestHandler extends SimpleChannelInboundHandler<PrepareStoreCopyRequest>
{
    private final CatchupServerProtocol protocol;
    private final PrepareStoreCopyFilesProvider prepareStoreCopyFilesProvider;
    private final Database db;
    private final StoreFileStreamingProtocol streamingProtocol;
    private final Log log;

    public PrepareStoreCopyRequestHandler( CatchupServerProtocol catchupServerProtocol, Database db,
            PrepareStoreCopyFilesProvider prepareStoreCopyFilesProvider, int maxChunkSize )
    {
        this.protocol = catchupServerProtocol;
        this.prepareStoreCopyFilesProvider = prepareStoreCopyFilesProvider;
        this.db = db;
        this.streamingProtocol = new StoreFileStreamingProtocol( maxChunkSize );
        this.log = db.getInternalLogProvider().getLog( getClass() );
    }

    @Override
    protected void channelRead0( ChannelHandlerContext channelHandlerContext, PrepareStoreCopyRequest prepareStoreCopyRequest ) throws IOException
    {
        CloseablesListener closeablesListener = new CloseablesListener();
        PrepareStoreCopyResponse response = PrepareStoreCopyResponse.error( PrepareStoreCopyResponse.Status.E_LISTING_STORE );
        try
        {
            if ( !canPrepareForStoreCopy( db ) )
            {
                return;
            }

            if ( !Objects.equals( prepareStoreCopyRequest.storeId(), db.getStoreId() ) )
            {
                response = PrepareStoreCopyResponse.error( PrepareStoreCopyResponse.Status.E_STORE_ID_MISMATCH );
            }
            else
            {
                CheckPointer checkPointer = db.getDependencyResolver().resolveDependency( CheckPointer.class );
                closeablesListener.add( tryCheckpointAndAcquireMutex( checkPointer ) );
                PrepareStoreCopyFiles prepareStoreCopyFiles = closeablesListener.add( prepareStoreCopyFilesProvider.prepareStoreCopyFiles( db ) );

                StoreResource[] nonReplayable = prepareStoreCopyFiles.getAtomicFilesSnapshot();
                for ( StoreResource storeResource : nonReplayable )
                {
                    streamingProtocol.stream( channelHandlerContext, storeResource );
                }
                response = createSuccessfulResponse( checkPointer, prepareStoreCopyFiles );
            }
        }
        finally
        {
            channelHandlerContext.write( ResponseMessageType.PREPARE_STORE_COPY_RESPONSE );
            channelHandlerContext.writeAndFlush( response ).addListener( closeablesListener );
            protocol.expect( CatchupServerProtocol.State.MESSAGE_TYPE );
        }
    }

    private PrepareStoreCopyResponse createSuccessfulResponse( CheckPointer checkPointer, PrepareStoreCopyFiles prepareStoreCopyFiles ) throws IOException
    {
        Path[] files = prepareStoreCopyFiles.listReplayableFiles();
        long lastCheckPointedTransactionId = checkPointer.lastCheckPointedTransactionId();
        return PrepareStoreCopyResponse.success( files, lastCheckPointedTransactionId );
    }

    private Resource tryCheckpointAndAcquireMutex( CheckPointer checkPointer ) throws IOException
    {
        return db.getStoreCopyCheckPointMutex().storeCopy( () -> checkPointer.tryCheckPoint( new SimpleTriggerInfo( "Store copy" ) ) );
    }

    private boolean canPrepareForStoreCopy( Database db )
    {
        if ( !db.getDatabaseAvailabilityGuard().isAvailable() )
        {
            log.warn( "Unable to prepare for store copy because database '" + db.getNamedDatabaseId().name() + "' is unavailable" );
            return false;
        }
        return true;
    }
}
