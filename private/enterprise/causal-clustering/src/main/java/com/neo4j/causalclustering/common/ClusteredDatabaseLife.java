/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Instances of this type encapsulate the lifecycle control for all components required by a
 * clustered database instance, including the Raft/Catchup servers and the kernel {@link Database}.
 *
 * Instances of this type should be thought of as Clustered versions of {@link Database}.
 *
 * Note: These instances only *appear* to be like {@link Lifecycle}s, due to providing start/stop methods.
 * In fact, instances of this interface are only ever managed directly by a {@link DatabaseManager},
 * never by a {@link LifeSupport}.
 *
 * If these instances were to implement the {@link Lifecycle} interface and added to a {@link LifeSupport},
 * the behaviour would be undefined.
 */
public abstract class ClusteredDatabaseLife
{
    private boolean started;
    private boolean initialized;

    public final void start() throws Exception
    {
        if ( !started )
        {
            start0();
            started = true;
            initialized = true;
        }
    }

    protected abstract void start0() throws Exception;

    public final void stop() throws Exception
    {
        if ( started )
        {
            stop0();
            started = false;
        }
    }

    protected abstract void stop0() throws Exception;

    public final boolean initialized()
    {
        return initialized;
    }
}
