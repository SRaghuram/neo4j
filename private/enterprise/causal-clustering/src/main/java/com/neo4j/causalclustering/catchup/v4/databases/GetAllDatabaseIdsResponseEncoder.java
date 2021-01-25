/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.databases;

import com.neo4j.causalclustering.discovery.akka.marshal.DatabaseIdWithoutNameMarshal;
import com.neo4j.causalclustering.messaging.NetworkWritableChannel;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import org.neo4j.kernel.database.NamedDatabaseId;

public class GetAllDatabaseIdsResponseEncoder extends MessageToByteEncoder<GetAllDatabaseIdsResponse>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, GetAllDatabaseIdsResponse response, ByteBuf out ) throws Exception
    {
        final var channel = new NetworkWritableChannel( out );
        channel.putLong( response.size() );

        for ( NamedDatabaseId namedDatabaseId : response.databaseIds() )
        {
            StringMarshal.marshal( channel, namedDatabaseId.name() );
            DatabaseIdWithoutNameMarshal.INSTANCE.marshal( namedDatabaseId.databaseId(), channel );
        }
    }
}
