/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core;

import com.neo4j.causalclustering.common.ClusteredDatabase;
import com.neo4j.causalclustering.core.consensus.RaftMachine;
import com.neo4j.causalclustering.core.state.CommandApplicationProcess;
import com.neo4j.causalclustering.core.state.snapshot.CoreDownloaderService;
import com.neo4j.causalclustering.messaging.LifecycleMessageHandler;

import java.util.List;

import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.recovery.RecoveryFacade;

import static org.neo4j.kernel.lifecycle.LifecycleAdapter.onStart;
import static org.neo4j.kernel.lifecycle.LifecycleAdapter.onStop;
import static org.neo4j.kernel.lifecycle.LifecycleAdapter.simpleLife;

/**
 * This class exists to give a nice and simple overview of Core database components, their startup
 * and shutdown order, and allow them to be handled independently as far as error handling goes.
 *
 * The starting order is read from top to bottom and the shutdown order in the opposite direction.
 * Note that every component is a {@link Lifecycle} and might only be active in certain stages.
 *
 * The clusterComponents parameter bundles a bunch of components together and this might be a prime
 * candidate for refactoring at a later stage to give an easy overview of them as well.
 */
class CoreDatabase extends ClusteredDatabase
{
    CoreDatabase( RaftMachine raftMachine, Database kernelDatabase, CommandApplicationProcess commandApplicationProcess,
            LifecycleMessageHandler<?> raftMessageHandler, CoreDownloaderService downloadService, RecoveryFacade recoveryFacade,
            CorePanicHandlers panicHandler, RaftStarter raftStarter, Lifecycle topologyComponents )
    {
        super( List.of( panicHandler,
                        onStart( () -> recoveryFacade.recovery( kernelDatabase.getDatabaseLayout() ) ),
                        topologyComponents,
                        raftStarter ),
               kernelDatabase,
               List.of( simpleLife( commandApplicationProcess::start, commandApplicationProcess::stop ),
                        onStart( raftMachine::postRecoveryActions ),
                        onStop( raftMessageHandler::stop ),
                        onStop( raftMachine::stopTimers ),
                        downloadService ) );
    }
}
