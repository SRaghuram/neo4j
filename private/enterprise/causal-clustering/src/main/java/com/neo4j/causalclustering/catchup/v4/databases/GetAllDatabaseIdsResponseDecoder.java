/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup.v4.databases;

import com.neo4j.causalclustering.discovery.akka.marshal.DatabaseIdWithoutNameMarshal;
import com.neo4j.causalclustering.messaging.NetworkReadableChannel;
import com.neo4j.causalclustering.messaging.marshalling.StringMarshal;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

public class GetAllDatabaseIdsResponseDecoder extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
    {
        final var channel = new NetworkReadableChannel( in );
        final var size = channel.getLong();
        final Set<NamedDatabaseId> databaseIds = new HashSet<>();

        for ( int i = 0; i < size; i++ )
        {
            final var name = StringMarshal.unmarshal( channel );
            final var databaseId = DatabaseIdWithoutNameMarshal.INSTANCE.unmarshal( channel );
            databaseIds.add( DatabaseIdFactory.from( name, databaseId.uuid() ) );
        }

        out.add( new GetAllDatabaseIdsResponse( databaseIds ) );
    }
}
