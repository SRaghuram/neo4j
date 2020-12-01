/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.error_handling.DatabasePanicEventHandler;
import com.neo4j.causalclustering.error_handling.PanicService;

import java.util.List;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public abstract class DatabasePanicHandlers extends LifecycleAdapter
{
    private final PanicService panicService;
    private final NamedDatabaseId namedDatabaseId;
    private final List<? extends DatabasePanicEventHandler> panicHandlerList;

    protected DatabasePanicHandlers( PanicService panicService, NamedDatabaseId namedDatabaseId, List<? extends DatabasePanicEventHandler> panicHandlerList )
    {
        this.panicService = panicService;
        this.namedDatabaseId = namedDatabaseId;
        this.panicHandlerList = panicHandlerList;
    }

    @Override
    public void init()
    {
        panicService.addDatabasePanicEventHandlers( namedDatabaseId, panicHandlerList );
    }

    @Override
    public void shutdown()
    {
        panicService.removeDatabasePanicEventHandlers( namedDatabaseId );
    }
}
