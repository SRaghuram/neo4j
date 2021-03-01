/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.readreplica;

import com.neo4j.causalclustering.catchup.CatchupComponentsRepository;
import com.neo4j.dbms.database.ClusteredDatabaseContext;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.NamedDatabaseId;

/**
 * Useful for getting catchup components for a database at a later stage. Can only be used after the {@link ClusteredDatabaseContext} for the database
 * has been created.
 */
public class CatchupComponentsProvider
{
    private final CatchupComponentsRepository catchupComponentsRepository;
    private final NamedDatabaseId databaseId;

    public CatchupComponentsProvider( CatchupComponentsRepository catchupComponentsRepository, NamedDatabaseId databaseId )
    {
        this.catchupComponentsRepository = catchupComponentsRepository;
        this.databaseId = databaseId;
    }

    /**
     * @return CatchupComponents from the {@link ClusteredDatabaseContext} for this database
     * @throws IllegalStateException if the {@link ClusteredDatabaseContext} has not been created yet.
     */
    CatchupComponentsRepository.CatchupComponents catchupComponents()
    {
        return catchupComponentsRepository.componentsFor( databaseId ).orElseThrow(
                () -> new IllegalStateException( String.format( "No CatchupComponents instance exists for database %s.", databaseId ) ) );
    }
}
