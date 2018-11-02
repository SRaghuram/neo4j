/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.handlers;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;

import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Intended to provide handlers which can wrap an entire sub-pipeline in a neutral
 * fashion, e.g compression, integrity checks, ...
 */
public interface PipelineWrapper
{
    @SuppressWarnings( "RedundantThrows" )
    default List<ChannelHandler> handlersFor( Channel channel ) throws Exception
    {
        return emptyList();
    }

    String name();
}
