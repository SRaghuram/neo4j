/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler;
import com.neo4j.causalclustering.error_handling.PanicService;

import java.util.List;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public abstract class DatabasePanicHandlers extends LifecycleAdapter
{
    private final PanicService panicService;
    private final DatabaseId databaseId;
    private final List<? extends DatabasePanicEventHandler> panicHandlerList;

    protected DatabasePanicHandlers( PanicService panicService, DatabaseId databaseId, List<? extends DatabasePanicEventHandler> panicHandlerList )
    {
        this.panicService = panicService;
        this.databaseId = databaseId;
        this.panicHandlerList = panicHandlerList;
    }

    @Override
    public void init()
    {
        panicService.addPanicEventHandlers( databaseId, panicHandlerList );
    }

    @Override
    public void shutdown()
    {
        panicService.removePanicEventHandlers( databaseId );
    }
}
