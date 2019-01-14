/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.rest;

import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;

public class CoreDatabaseAvailabilityDiscoveryRepresentation extends MappingRepresentation
{
    private static final String WRITABLE_KEY = "writable";
    private static final String DISCOVERY_REPRESENTATION_TYPE = "discovery";

    private final String basePath;
    private final String isWritableUri;

    public CoreDatabaseAvailabilityDiscoveryRepresentation( String basePath, String isWritableUri )
    {
        super( DISCOVERY_REPRESENTATION_TYPE );
        this.basePath = basePath;
        this.isWritableUri = isWritableUri;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        serializer.putRelativeUri( WRITABLE_KEY, basePath + isWritableUri );
    }
}
