/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.readreplica;

import java.util.function.BooleanSupplier;

import org.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.neo4j.causalclustering.common.AbstractLocalDatabase;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.state.DataSourceManager;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

public class ReadReplicaLocalDatabase extends AbstractLocalDatabase
{

    ReadReplicaLocalDatabase( String databaseName, DataSourceManager dataSourceManager, DatabaseLayout databaseLayout, LogFiles txLogs,
            StoreFiles storeFiles, LogProvider logProvider, BooleanSupplier isAvailable, JobScheduler jobScheduler )
    {
        super( databaseName, dataSourceManager, databaseLayout, txLogs, storeFiles, logProvider, isAvailable, jobScheduler );
    }

    @Override
    public void start0()
    {
        //no-op
    }
}
