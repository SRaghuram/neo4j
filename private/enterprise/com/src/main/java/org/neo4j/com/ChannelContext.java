/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;

import java.nio.ByteBuffer;

import static java.util.Objects.requireNonNull;

public class ChannelContext
{
    private final Channel channel;
    private final ChannelBuffer output;
    private final ByteBuffer input;

    public ChannelContext( Channel channel, ChannelBuffer output, ByteBuffer input )
    {
        this.channel = requireNonNull( channel );
        this.output = requireNonNull( output );
        this.input = requireNonNull( input );
    }

    public Channel channel()
    {
        return channel;
    }

    public ChannelBuffer output()
    {
        return output;
    }

    public ByteBuffer input()
    {
        return input;
    }

    @Override
    public String toString()
    {
        return "ChannelContext{channel=" + channel + ", output=" + output + ", input=" + input + "}";
    }
}
