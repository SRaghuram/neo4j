/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.procedures.wait;

import java.util.LinkedList;
import java.util.Queue;

import static java.util.stream.LongStream.range;

class WindowedTimeout
{
    public static Builder builder()
    {
        return new Builder();
    }

    private final Queue<Long> timeouts;
    private final long lastTimeout;

    WindowedTimeout( Queue<Long> timeouts, long lastTimeout )
    {
        this.timeouts = timeouts;
        this.lastTimeout = lastTimeout;
    }

    public long nextTimeout()
    {
        if ( timeouts.isEmpty() )
        {
            return lastTimeout;
        }
        return timeouts.poll();
    }

    public static class Builder
    {
        private final Queue<Long> timeouts = new LinkedList<>();

        public Builder nextWindow( long timeoutMs, int amount )
        {
            range( 0, amount ).forEach( ignore -> timeouts.add( timeoutMs ) );
            return this;
        }

        public WindowedTimeout build( long finalTimeoutMs )
        {
            return new WindowedTimeout( timeouts, finalTimeoutMs );
        }
    }
}
