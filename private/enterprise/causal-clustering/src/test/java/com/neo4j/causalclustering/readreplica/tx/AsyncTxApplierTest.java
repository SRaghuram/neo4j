/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica.tx;

import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.jupiter.api.Test;

import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.logging.NullLogProvider;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncTxApplierTest
{
    @Test
    void shouldExecuteQueuedJobs() throws Exception
    {
        var initialisedScheduler = JobSchedulerFactory.createInitialisedScheduler();
        MutableInt counter = new MutableInt();
        var asyncTxApplier = new AsyncTxApplier( initialisedScheduler, NullLogProvider.nullLogProvider() );

        asyncTxApplier.add( counter::increment );

        assertThat( counter.getValue() ).isEqualTo( 0 );

        asyncTxApplier.start();
        asyncTxApplier.add( counter::increment );
        asyncTxApplier.add( counter::increment );
        asyncTxApplier.stop();

        assertThat( counter.getValue() ).isEqualTo( 3 );

        initialisedScheduler.shutdown();
    }
}
