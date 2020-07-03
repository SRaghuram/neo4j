/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.storecopy;

import com.neo4j.causalclustering.catchup.ResponseMessageType;
import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;

public class StoreFileStreamingProtocol
{
    private final int maxChunkSize;

    public StoreFileStreamingProtocol( int maxChunkSize )
    {
        this.maxChunkSize = maxChunkSize;
    }

    /**
     * This sends operations on the outgoing pipeline or the file, including
     * chunking {@link FileSender} handlers.
     * <p>
     * Note that we do not block here.
     */
    public void stream( ChannelHandlerContext ctx, StoreResource resource )
    {
        ctx.write( ResponseMessageType.FILE );
        ctx.write( new FileHeader( resource.relativePath(), resource.recordSize() ) );
        ctx.write( new FileSender( resource, maxChunkSize ) );
    }

    public Future<Void> end( ChannelHandlerContext ctx, StoreCopyFinishedResponse.Status status, long lastCheckpointedTx )
    {
        ctx.write( ResponseMessageType.STORE_COPY_FINISHED );
        return ctx.writeAndFlush( new StoreCopyFinishedResponse( status, lastCheckpointedTx ) );
    }
}
