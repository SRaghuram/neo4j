/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.PerDatabaseCatchupComponents;
import com.neo4j.causalclustering.common.LocalDatabase;

public interface CatchupComponentsFactory
{
    PerDatabaseCatchupComponents createPerDatabaseComponents( LocalDatabase localDatabase );
}
