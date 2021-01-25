/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.common.ClusteredDatabase;
import com.neo4j.causalclustering.common.DatabaseTopologyNotifier;

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
    ReadReplicaDatabase( CatchupProcessManager catchupProcess, Database kernelDatabase, Lifecycle clusterComponents, ReadReplicaBootstrap bootstrap,
            ReadReplicaPanicHandlers panicHandler, RaftIdCheck raftIdCheck, DatabaseTopologyNotifier topologyNotifier )
    {
        addComponent( panicHandler );
        addComponent( onInit( raftIdCheck::perform ) );

        addComponent( clusterComponents );
        addComponent( topologyNotifier );

        addComponent( onStart( bootstrap::perform ) );

        addComponent( kernelDatabase );
        addComponent( catchupProcess );
    }
}
