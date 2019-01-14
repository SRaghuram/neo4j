/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.helper;

import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.logging.Log;

public abstract class SuspendableLifeCycle implements Lifecycle, Suspendable
{
    private final Log debugLog;
    private boolean stoppedByLifeCycle = true;
    private boolean enabled = true;

    public SuspendableLifeCycle( Log debugLog )
    {
        this.debugLog = debugLog;
    }

    @Override
    public final synchronized void enable() throws Throwable
    {
        if ( !stoppedByLifeCycle )
        {
            start0();
        }
        else
        {
            debugLog.info( "%s will not start. It was enabled but is stopped by lifecycle", this );
        }
        enabled = true;
    }

    @Override
    public final synchronized void disable() throws Throwable
    {
        stop0();
        enabled = false;
    }

    @Override
    public final synchronized void init() throws Throwable
    {
        init0();
    }

    @Override
    public final synchronized void start() throws Throwable
    {
        if ( !enabled )
        {
            debugLog.info( "Start call from lifecycle is ignored because %s is disabled.", this );
        }
        else
        {
            start0();
        }
        stoppedByLifeCycle = false;
    }

    @Override
    public final synchronized void stop() throws Throwable
    {
        stop0();
        stoppedByLifeCycle = true;
    }

    @Override
    public final synchronized void shutdown() throws Throwable
    {
        shutdown0();
        stoppedByLifeCycle = true;
    }

    protected abstract void init0() throws Throwable;

    protected abstract void start0() throws Throwable;

    protected abstract void stop0() throws Throwable;

    protected abstract void shutdown0() throws Throwable;
}
