/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.dbms.database.EnterpriseDatabase;

import java.util.List;

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
 */
public class ClusteredDatabase extends EnterpriseDatabase
{
    private boolean hasBeenStarted;

    protected ClusteredDatabase( List<Lifecycle> components )
    {
        super( components );
    }

    @Override
    public void start()
    {
        if ( hasBeenStarted )
        {
            throw new IllegalStateException( "Clustered databases do not support component reuse." );
        }
        hasBeenStarted = true;
        super.start();
    }

    @Override
    public void stop()
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
}
