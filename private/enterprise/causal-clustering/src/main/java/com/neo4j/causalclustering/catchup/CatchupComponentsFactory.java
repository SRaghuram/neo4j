/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import com.neo4j.causalclustering.catchup.CatchupComponentsRepository.CatchupComponents;
import com.neo4j.dbms.database.ClusteredDatabaseContext;

@FunctionalInterface
public interface CatchupComponentsFactory
{
    CatchupComponents createDatabaseComponents( ClusteredDatabaseContext clusteredDatabaseContext );
}
