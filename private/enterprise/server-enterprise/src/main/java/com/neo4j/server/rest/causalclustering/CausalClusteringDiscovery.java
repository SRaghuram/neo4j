/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;

import static com.neo4j.server.rest.causalclustering.CausalClusteringService.AVAILABLE;
import static com.neo4j.server.rest.causalclustering.CausalClusteringService.READ_ONLY;
import static com.neo4j.server.rest.causalclustering.CausalClusteringService.STATUS;
import static com.neo4j.server.rest.causalclustering.CausalClusteringService.WRITABLE;

public class CausalClusteringDiscovery extends MappingRepresentation
{
    private static final String DISCOVERY_REPRESENTATION_TYPE = "discovery";

    private final String databasePath;

    CausalClusteringDiscovery( String databasePath )
    {
        super( DISCOVERY_REPRESENTATION_TYPE );
        this.databasePath = databasePath;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        serializer.putRelativeUri( AVAILABLE, databasePath + "/" + AVAILABLE );
        serializer.putRelativeUri( READ_ONLY, databasePath + "/" + READ_ONLY );
        serializer.putRelativeUri( WRITABLE, databasePath + "/" + WRITABLE );
        serializer.putRelativeUri( STATUS, databasePath + "/" + STATUS );
    }
}
