/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging.marshalling;

import com.neo4j.causalclustering.catchup.Protocol;
import com.neo4j.causalclustering.core.replication.ReplicatedContent;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

public class ReplicatedContentDecoder extends MessageToMessageDecoder<ContentBuilder<ReplicatedContent>>
{
    private final Protocol<ContentType> protocol;
    private ContentBuilder<ReplicatedContent> contentBuilder = ContentBuilder.emptyUnfinished();

    public ReplicatedContentDecoder( Protocol<ContentType> protocol )
    {
        this.protocol = protocol;
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ContentBuilder<ReplicatedContent> msg, List<Object> out )
    {
        contentBuilder.combine( msg );
        if ( contentBuilder.isComplete() )
        {
            out.add( contentBuilder.build() );
            contentBuilder = ContentBuilder.emptyUnfinished();
            protocol.expect( ContentType.ContentType );
        }
    }
}
