/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.catchup;

import org.neo4j.causalclustering.catchup.CatchupComponentsRepository.PerDatabaseCatchupComponents;
import org.neo4j.causalclustering.common.LocalDatabase;

public interface CatchupComponentsFactory
{
    PerDatabaseCatchupComponents createPerDatabaseComponents( String databaseName, LocalDatabase localDatabase );
}
