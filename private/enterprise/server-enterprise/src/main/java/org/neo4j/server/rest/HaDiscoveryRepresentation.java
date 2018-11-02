/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.rest;

import org.neo4j.server.rest.repr.MappingRepresentation;
import org.neo4j.server.rest.repr.MappingSerializer;

public class HaDiscoveryRepresentation extends MappingRepresentation
{
    private static final String MASTER_KEY = "master";
    private static final String SLAVE_KEY = "slave";
    private static final String DISCOVERY_REPRESENTATION_TYPE = "discovery";

    private final String basePath;
    private final String isMasterUri;
    private final String isSlaveUri;

    public HaDiscoveryRepresentation( String basePath, String isMasterUri, String isSlaveUri )
    {
        super( DISCOVERY_REPRESENTATION_TYPE );
        this.basePath = basePath;
        this.isMasterUri = isMasterUri;
        this.isSlaveUri = isSlaveUri;
    }

    @Override
    protected void serialize( MappingSerializer serializer )
    {
        serializer.putRelativeUri( MASTER_KEY, basePath + isMasterUri );
        serializer.putRelativeUri( SLAVE_KEY, basePath + isSlaveUri );
    }
}
