/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.function.Consumer;
import java.util.function.Function;

import org.neo4j.kernel.database.NamedDatabaseId;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * This class connects the Database lifecycle to the TopologyService
 *
 * If a database is running in a clustered environment, it's existence needs to be reported to Topology. It is needed, because this is how other
 * instances will know that this particular instance hosts this database.
 * In order to do that when the Database as Lifecycle receives the start and the stop signals it publishes and respectively
 * un-publishes itself to/from the topology.
 */
public abstract class TopologyPublisher extends LifecycleAdapter
{
    public interface Factory extends Function<NamedDatabaseId,TopologyPublisher>
    {
    }

    public static TopologyPublisher from( NamedDatabaseId namedDatabaseId, Consumer<NamedDatabaseId> publish, Consumer<NamedDatabaseId> unPublish )
    {
        return new TopologyPublisher()
        {
            @Override
            protected void publish()
            {
                publish.accept( namedDatabaseId );
            }

            @Override
            protected void unPublish()
            {
                unPublish.accept( namedDatabaseId );
            }
        };
    }

    public static final TopologyPublisher NOOP = new TopologyPublisher()
    {
        @Override
        protected void publish()
        {//no-op
        }

        @Override
        protected void unPublish()
        {//no-op
        }
    };

    protected abstract void publish();

    protected abstract void unPublish();

    @Override
    public void start()
    {
        publish();
    }

    @Override
    public void stop()
    {
        unPublish();
    }
}
