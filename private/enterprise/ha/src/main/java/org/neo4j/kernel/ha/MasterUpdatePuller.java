/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

/**
 * Masters implementation of update puller that does nothing since master should not pull updates.
 */
public class MasterUpdatePuller implements UpdatePuller
{

    public static final MasterUpdatePuller INSTANCE = new MasterUpdatePuller();

    private MasterUpdatePuller()
    {
    }

    @Override
    public void start()
    {
        // no-op
    }

    @Override
    public void stop()
    {
        // no-op
    }

    @Override
    public void pullUpdates()
    {
        // no-op
    }

    @Override
    public boolean tryPullUpdates()
    {
        return false;
    }

    @Override
    public void pullUpdates( Condition condition, boolean assertPullerActive )
    {
        // no-op
    }
}
