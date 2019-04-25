/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.messaging.marshalling.DatabaseIdMarshal;

import org.neo4j.kernel.database.DatabaseId;

public class DatabaseIdSerializer extends BaseAkkaSerializer<DatabaseId>
{
    private static final int sizeHint = 64;

    protected DatabaseIdSerializer()
    {
        super( DatabaseIdMarshal.INSTANCE, DATABASE_ID, sizeHint );
    }
}
