/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleException;

/**
 * Instances of this type encapsulate the lifecycle control for all components required by a
 * clustered database instance, including the Raft/Catchup servers and the kernel {@link Database}.
 *
 * Instances of this type should be thought of as Clustered versions of {@link Database}.
 *
 * Note: These instances only *appear* to be like {@link Lifecycle}s, due to providing start/stop methods.
 * In fact, instances of this interface are only ever managed directly by a {@link DatabaseManager},
 * never by a {@link LifeSupport}.
 */
public class ClusteredDatabase
{
    private final LifeSupport components = new LifeSupport();
    private boolean hasBeenStarted;

    public final void start()
    {
        if ( hasBeenStarted )
        {
            throw new IllegalStateException( "Clustered databases do not support component reuse." );
        }
        hasBeenStarted = true;

        try
        {
            components.start();
        }
        catch ( LifecycleException startException )
        {
            // LifeSupport will stop() on failure, but not shutdown()
            try
            {
                components.shutdown();
            }
            catch ( Throwable shutdownException )
            {
                startException.addSuppressed( shutdownException );
            }

            throw startException;
        }
    }

    public final void stop()
    {
        components.shutdown();
    }

    /**
     * Returns true if this database ever was attempted to be started (even if it failed to start or was later shutdown).
     */
    public final boolean hasBeenStarted()
    {
        return hasBeenStarted;
    }

    protected void addComponent( Lifecycle component )
    {
        components.add( component );
    }
}
