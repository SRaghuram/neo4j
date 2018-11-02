/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.timeout;

import org.neo4j.cluster.com.message.Message;

/**
 * A {@link TimeoutStrategy} that sets timeouts to a given fixed value.
 */
public class FixedTimeoutStrategy
    implements TimeoutStrategy
{

    protected final long timeout;

    public FixedTimeoutStrategy( long timeout )
    {
        this.timeout = timeout;
    }

    @Override
    public long timeoutFor( Message message )
    {
        return timeout;
    }

    @Override
    public void timeoutTriggered( Message timeoutMessage )
    {
    }

    @Override
    public void timeoutCancelled( Message timeoutMessage )
    {
    }

    @Override
    public void tick( long now )
    {
    }
}
