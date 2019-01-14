/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.readreplica;

import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.neo4j.causalclustering.catchup.storecopy.StoreFiles;
import org.neo4j.causalclustering.common.AbstractLocalDatabase;
import org.neo4j.dbms.database.DatabaseManager;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.LogProvider;

public class ReadReplicaLocalDatabase extends AbstractLocalDatabase
{
    ReadReplicaLocalDatabase( String databaseName, Supplier<DatabaseManager> databaseManagerSupplier, DatabaseLayout databaseLayout, LogFiles txLogs,
            StoreFiles storeFiles, LogProvider logProvider, BooleanSupplier isAvailable )
    {
        super( databaseName, databaseManagerSupplier, databaseLayout, txLogs, storeFiles, logProvider, isAvailable );
    }

    @Override
    public void start0()
    {
        //no-op
    }
}
