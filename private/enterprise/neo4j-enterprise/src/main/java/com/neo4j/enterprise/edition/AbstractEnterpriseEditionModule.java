/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.enterprise.edition;

import com.neo4j.causalclustering.core.CoreEditionModule;
import com.neo4j.causalclustering.readreplica.ReadReplicaEditionModule;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.cypher.internal.runtime.pipelined.WorkerManager;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.scheduler.Group;

public interface AbstractEnterpriseEditionModule
{
    /**
     * Satisfy any enterprise only dependencies, that are also needed in other Editions,
     * e.g. {@link CoreEditionModule} and {@link ReadReplicaEditionModule}.
     */
    default void satisfyEnterpriseOnlyDependencies( GlobalModule globalModule )
    {
        // Create Cypher workers
        Config globalConfig = globalModule.getGlobalConfig();
        int configuredWorkers = globalConfig.get( GraphDatabaseSettings.cypher_worker_count );
        int numberOfThreads = configuredWorkers == 0 ? Runtime.getRuntime().availableProcessors() : configuredWorkers;
        WorkerManager workerManager =
                new WorkerManager( numberOfThreads, globalModule.getJobScheduler().threadFactory( Group.CYPHER_WORKER ) );
        globalModule.getGlobalDependencies().satisfyDependency( workerManager );
        globalModule.getGlobalLife().add( workerManager );
    }
}
