/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.messaging;

public interface PoolEventsListener
{
    PoolEventsListener EMPTY_LISTENER = new PoolEventsListener()
    {
        @Override
        public void onChannelAcquired()
        {
            // do nothing
        }

        @Override
        public void onChannelReleased()
        {
            // do nothing
        }

        @Override
        public void onChannelCreated()
        {
            // do nothing
        }
    };

    void onChannelAcquired();

    void onChannelReleased();

    void onChannelCreated();
}
