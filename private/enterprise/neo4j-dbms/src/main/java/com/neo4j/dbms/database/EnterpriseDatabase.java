/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms.database;

import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

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
public class EnterpriseDatabase
{
    protected final LifeSupport components = new LifeSupport();

    protected EnterpriseDatabase( List<Lifecycle> components )
    {
        components.forEach( this.components::add );
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

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        EnterpriseDatabase that = (EnterpriseDatabase) o;
        return components.equals( that.components );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( components );
    }

    public static <T extends EnterpriseDatabase> WithBeforeOrKernel<T> builder( Function<List<Lifecycle>,T> constructor )
    {
        return new BeforeBuilder<>( constructor );
    }

    public interface WithBeforeOrKernel<T extends EnterpriseDatabase>
    {
        WithBeforeOrKernel<T> withComponent( Lifecycle component );

        WithAfterOrBuild<T> withKernelDatabase( Database kernelDatabase );
    }

    public interface WithAfterOrBuild<T extends EnterpriseDatabase>
    {
        WithAfterOrBuild<T> withComponent( Lifecycle component );

        T build();
    }

    private static class BeforeBuilder<T extends EnterpriseDatabase> implements WithBeforeOrKernel<T>
    {
        private final Function<List<Lifecycle>,T> constructor;
        private final List<Lifecycle> components;

        private BeforeBuilder( Function<List<Lifecycle>,T> constructor )
        {
            this.constructor = constructor;
            this.components = new LinkedList<>();
        }

        @Override
        public WithBeforeOrKernel<T> withComponent( Lifecycle component )
        {
            this.components.add( component );
            return this;
        }

        @Override
        public WithAfterOrBuild<T> withKernelDatabase( Database kernelDatabase )
        {
            this.components.add( kernelDatabase );
            return new AfterBuilder<>( constructor, components );
        }
    }

    private static class AfterBuilder<T extends EnterpriseDatabase> implements WithAfterOrBuild<T>
    {
        private final List<Lifecycle> components;
        private final Function<List<Lifecycle>,T> constructor;

        private AfterBuilder( Function<List<Lifecycle>,T> constructor, List<Lifecycle> components )
        {
            this.constructor = constructor;
            this.components = components;
        }

        @Override
        public WithAfterOrBuild<T> withComponent( Lifecycle component )
        {
            this.components.add( component );
            return this;
        }

        @Override
        public T build()
        {
            return constructor.apply( this.components );
        }

    }
}
