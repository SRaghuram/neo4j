/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.channel.ChannelHandlerContext;
import io.netty.util.concurrent.Future;

import org.neo4j.causalclustering.catchup.ResponseMessageType;

public class StoreFileStreamingProtocol
{
    /**
     * This sends operations on the outgoing pipeline or the file, including
     * chunking {@link org.neo4j.causalclustering.catchup.storecopy.FileSender} handlers.
     * <p>
     * Note that we do not block here.
     */
    void stream( ChannelHandlerContext ctx, StoreResource resource )
    {
        ctx.write( ResponseMessageType.FILE );
        ctx.write( new FileHeader( resource.path(), resource.recordSize() ) );
        ctx.write( new FileSender( resource ) );
    }

    Future<Void> end( ChannelHandlerContext ctx, StoreCopyFinishedResponse.Status status )
    {
        ctx.write( ResponseMessageType.STORE_COPY_FINISHED );
        return ctx.writeAndFlush( new StoreCopyFinishedResponse( status ) );
    }
}
