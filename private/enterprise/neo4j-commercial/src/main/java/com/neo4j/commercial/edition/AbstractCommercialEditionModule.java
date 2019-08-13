/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.commercial.edition;

import com.neo4j.causalclustering.core.CoreEditionModule;
import com.neo4j.causalclustering.readreplica.ReadReplicaEditionModule;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.runtime.morsel.ThrowingWorkerManager$;
import org.neo4j.cypher.internal.runtime.morsel.WorkerManagement;
import org.neo4j.cypher.internal.runtime.morsel.WorkerManager;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.scheduler.Group;

public interface AbstractCommercialEditionModule
{
    /**
     * Satisfy any commercial only dependencies, that are also needed in other Editions,
     * e.g. {@link CoreEditionModule} and {@link ReadReplicaEditionModule}.
     */
    default void satisfyCommercialOnlyDependencies( GlobalModule globalModule )
    {
        // Create Cypher workers
        Config globalConfig = globalModule.getGlobalConfig();
        if ( globalConfig.get( GraphDatabaseSettings.cypher_morsel_runtime_scheduler ) !=
             GraphDatabaseSettings.CypherMorselRuntimeScheduler.SINGLE_THREADED )
        {
            int configuredWorkers = globalConfig.get( GraphDatabaseSettings.cypher_worker_count );
            int numberOfThreads = configuredWorkers == 0 ? Runtime.getRuntime().availableProcessors() : configuredWorkers;
            WorkerManager workerManager =
                    new WorkerManager( numberOfThreads, globalModule.getJobScheduler().threadFactory( Group.CYPHER_WORKER ) );
            globalModule.getGlobalDependencies().satisfyDependency( workerManager );
            globalModule.getGlobalLife().add( workerManager );
        }
        else
        {
            WorkerManagement workerManagement = ThrowingWorkerManager$.MODULE$;
            globalModule.getGlobalDependencies().satisfyDependency( workerManagement );
        }
    }
}
