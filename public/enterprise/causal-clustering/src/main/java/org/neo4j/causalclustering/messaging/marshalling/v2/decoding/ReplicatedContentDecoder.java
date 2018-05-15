/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.messaging.marshalling.v2.decoding;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.neo4j.causalclustering.catchup.Protocol;
import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.causalclustering.messaging.marshalling.v2.ContentType;
import org.neo4j.causalclustering.messaging.marshalling.v2.CoreReplicatedContentSerializer;

public class ReplicatedContentDecoder extends ByteToMessageDecoder
{
    private final CoreReplicatedContentSerializer coreReplicatedContentSerializer = new CoreReplicatedContentSerializer();
    private final Protocol<ContentType> protocol;
    private CoreReplicatedContentSerializer.ReplicatedContentBuilder replicatedContentBuilder =
            CoreReplicatedContentSerializer.ReplicatedContentBuilder.emptyUnfinished();

    public ReplicatedContentDecoder( Protocol<ContentType> protocol )
    {
        this.protocol = protocol;
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
    {
        replicatedContentBuilder.combine( coreReplicatedContentSerializer.decode( new NetworkReadableClosableChannelNetty4( in ) ) );
        if ( replicatedContentBuilder.isComplete() )
        {
            out.add( replicatedContentBuilder.build() );
            replicatedContentBuilder = CoreReplicatedContentSerializer.ReplicatedContentBuilder.emptyUnfinished();
            protocol.expect( ContentType.MessageType );
        }
    }
}
