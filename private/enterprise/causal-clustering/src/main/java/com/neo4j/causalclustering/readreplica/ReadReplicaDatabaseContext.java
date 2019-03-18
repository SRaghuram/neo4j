/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupComponentsFactory;
import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import com.neo4j.causalclustering.common.AbstractClusteredDatabaseContext;

import java.util.function.BooleanSupplier;

import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.LogProvider;

public class ReadReplicaDatabaseContext extends AbstractClusteredDatabaseContext
{
    ReadReplicaDatabaseContext( Database database, GraphDatabaseFacade facade, LogFiles txLogs, StoreFiles storeFiles,
            LogProvider logProvider, BooleanSupplier isAvailable, CatchupComponentsFactory catchupComponentsFactory )
    {
        super( database, facade, txLogs, storeFiles, logProvider, isAvailable, catchupComponentsFactory );
    }

    @Override
    protected void start0() throws Exception
    {
        //no-op
    }

    @Override
    protected void stop0() throws Exception
    {
        //no-op
    }
}
