/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;

import org.neo4j.kernel.database.NamedDatabaseId;

public class CatchupComponentsProvider
{
    private final CatchupComponentsRepository catchupComponentsRepository;
    private final NamedDatabaseId databaseId;

    public CatchupComponentsProvider( CatchupComponentsRepository catchupComponentsRepository, NamedDatabaseId databaseId )
    {
        this.catchupComponentsRepository = catchupComponentsRepository;
        this.databaseId = databaseId;
    }

    CatchupComponentsRepository.CatchupComponents getComponents()
    {
        return catchupComponentsRepository.componentsFor( databaseId ).orElseThrow(
                () -> new IllegalArgumentException( String.format( "No CatchupComponents instance exists for database %s.", databaseId ) ) );
    }
}
