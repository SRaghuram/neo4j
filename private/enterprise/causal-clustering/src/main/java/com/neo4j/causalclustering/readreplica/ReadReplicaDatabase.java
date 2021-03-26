/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.common.ClusteredDatabase;
import com.neo4j.dbms.DatabaseStartAborter;
import com.neo4j.dbms.TopologyPublisher;

import java.util.List;

import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.lifecycle.Lifecycle;

import static org.neo4j.kernel.lifecycle.LifecycleAdapter.onInit;
import static org.neo4j.kernel.lifecycle.LifecycleAdapter.onStart;

/**
 * This class exists to give a nice and simple overview of Read Replica database components, their startup
 * and shutdown order, and allow them to be handled independently as far as error handling goes.
 *
 * The starting order is read from top to bottom and the shutdown order in the opposite direction.
 * Note that every component is a {@link Lifecycle} and might only be active in certain stages.
 *
 * The clusterComponents parameter bundles a bunch of components together and this might be a prime
 * candidate for refactoring at a later stage to give an easy overview of them as well.
 */
class ReadReplicaDatabase extends ClusteredDatabase
{
    static ReadReplicaDatabase create( CatchupPollingProcess catchupPollingProcess, CatchupJobScheduler catchupJobScheduler, Database kernelDatabase,
            Lifecycle clusterComponents, ReadReplicaBootstrap bootstrap, ReadReplicaPanicHandlers panicHandler, RaftIdCheck raftIdCheck,
            TopologyPublisher topologyPublisher, DatabaseStartAborter databaseStartAborter )
    {
        return builder( ReadReplicaDatabase::new )
                .withComponent( onInit( () -> databaseStartAborter.resetFor( kernelDatabase.getNamedDatabaseId() ) ) )
                .withComponent( panicHandler )
                .withComponent( onInit( raftIdCheck::perform ) )
                .withComponent( clusterComponents )
                .withComponent( topologyPublisher )
                .withComponent( onStart( bootstrap::perform ) )
                .withKernelDatabase( kernelDatabase )
                .withComponent( catchupPollingProcess )
                .withComponent( catchupJobScheduler )
                .build();
    }

    private ReadReplicaDatabase( List<Lifecycle> components )
    {
        super( components );
    }
}
