/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import java.util.List;

import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleException;

/**
 * Instances of this type encapsulate the lifecycle control for all components required by a
 * database instance, most importantly the kernel {@link Database}, but also components needed by different editions.
 *
 * Note: These instances only *appear* to be like {@link Lifecycle}s, due to providing start/stop methods.
 * In fact, instances of this interface are only ever managed directly by a {@link DatabaseManager},
 * never by a {@link LifeSupport}.
 */
public class CompositeDatabase
{
    protected final LifeSupport components = new LifeSupport();

    public CompositeDatabase( List<Lifecycle> beforeKernelComponents, Database kernelDatabase, List<Lifecycle> afterKernelComponents )
    {
        beforeKernelComponents.forEach( components::add );
        components.add( kernelDatabase );
        afterKernelComponents.forEach( components::add );
    }

    public void start()
    {
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

    public void stop()
    {
        components.stop();
    }
}
