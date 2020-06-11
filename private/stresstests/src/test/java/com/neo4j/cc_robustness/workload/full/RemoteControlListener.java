/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload.full;

public interface RemoteControlListener
{
    void stopped();

    void pausedChanged( boolean paused );

    void killerPausedChanged( boolean paused );

    class Adapter implements RemoteControlListener
    {
        @Override
        public void stopped()
        {
        }

        @Override
        public void pausedChanged( boolean paused )
        {
        }

        @Override
        public void killerPausedChanged( boolean paused )
        {
        }
    }
}
