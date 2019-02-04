/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.common;

import com.neo4j.causalclustering.catchup.storecopy.StoreFiles;

import java.util.function.BooleanSupplier;

import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.LogProvider;

@FunctionalInterface
public interface ClusteredDatabaseContextFactory<DB extends ClusteredDatabaseContext>
{
     DB create( Database database, GraphDatabaseFacade facade, LogFiles txLogs, StoreFiles storeFiles, LogProvider logProvider, BooleanSupplier isAvailable );
}
