/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.error_handling;

import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class PanicEventHandlerLifecycle extends LifecycleAdapter
{
    private final PanicService parent;
    private final PanicEventHandler eventHandler;

    PanicEventHandlerLifecycle( PanicService parent, PanicEventHandler eventHandler )
    {
        this.parent = parent;
        this.eventHandler = eventHandler;
    }

    @Override
    public void start()
    {
        parent.add( eventHandler );
    }

    @Override
    public void stop()
    {
        parent.remove( eventHandler );
    }
}
