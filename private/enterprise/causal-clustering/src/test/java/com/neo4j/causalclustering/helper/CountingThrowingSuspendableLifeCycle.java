/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.helper;

import org.neo4j.logging.NullLog;

public class CountingThrowingSuspendableLifeCycle extends SuspendableLifeCycle
{
    public CountingThrowingSuspendableLifeCycle()
    {
        super( NullLog.getInstance() );
    }

    int starts;
    int stops;
    private boolean nextShouldFail;

    void setFailMode()
    {
        nextShouldFail = true;
    }

    void setSuccessMode()
    {
        nextShouldFail = false;
    }

    @Override
    protected void init0()
    {
        handleMode();
    }

    @Override
    protected void start0()
    {
        handleMode();
        starts++;
    }

    @Override
    protected void stop0()
    {
        handleMode();
        stops++;
    }

    @Override
    protected void shutdown0()
    {
        handleMode();
    }

    private void handleMode()
    {
        if ( nextShouldFail )
        {
            throw new IllegalStateException( "Tragedy" );
        }
    }
}
