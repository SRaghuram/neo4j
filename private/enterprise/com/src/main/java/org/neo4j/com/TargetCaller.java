/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import org.jboss.netty.buffer.ChannelBuffer;

public interface TargetCaller<T, R>
{
    Response<R> call( T requestTarget, RequestContext context, ChannelBuffer input, ChannelBuffer target ) throws Exception;
}
