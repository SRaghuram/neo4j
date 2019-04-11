/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v3.storecopy;

import com.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import com.neo4j.causalclustering.messaging.marshalling.DatabaseIdMarshal;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import com.neo4j.causalclustering.messaging.marshalling.storeid.StoreIdMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.io.File;
import java.util.List;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.util.Preconditions;

public class GetStoreFileRequestDecoder extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
    {
        NetworkReadableClosableChannelNetty4 channel = new NetworkReadableClosableChannelNetty4( in );
        DatabaseId databaseId = DatabaseIdMarshal.INSTANCE.unmarshal( channel );
        StoreId storeId = StoreIdMarshal.INSTANCE.unmarshal( channel );
        long requiredTransactionId = in.readLong();
        String fileName = StringMarshal.unmarshal( in );
        Preconditions.checkState( fileName != null, "Illegal request without a file name" );
        GetStoreFileRequest request = new GetStoreFileRequest( storeId, new File( fileName ), requiredTransactionId, databaseId );
        out.add( request );
    }
}
