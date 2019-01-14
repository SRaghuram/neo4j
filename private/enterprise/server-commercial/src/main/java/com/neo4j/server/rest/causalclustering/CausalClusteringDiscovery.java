/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.causalclustering;

import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;

import static com.neo4j.server.rest.causalclustering.CausalClusteringService.AVAILABLE;
import static com.neo4j.server.rest.causalclustering.CausalClusteringService.DESCRIPTION;
import static com.neo4j.server.rest.causalclustering.CausalClusteringService.READ_ONLY;
import static com.neo4j.server.rest.causalclustering.CausalClusteringService.WRITABLE;

public class CausalClusteringDiscovery extends MappingRepresentation
{
    private static final String DISCOVERY_REPRESENTATION_TYPE = "discovery";

    private final String basePath;

    CausalClusteringDiscovery( String basePath )
    {
        super( DISCOVERY_REPRESENTATION_TYPE );
        this.basePath = basePath;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        serializer.putRelativeUri( AVAILABLE, basePath + "/" + AVAILABLE );
        serializer.putRelativeUri( READ_ONLY, basePath + "/" + READ_ONLY );
        serializer.putRelativeUri( WRITABLE, basePath + "/" + WRITABLE );
        serializer.putRelativeUri( DESCRIPTION, basePath + "/" + DESCRIPTION );
    }
}
