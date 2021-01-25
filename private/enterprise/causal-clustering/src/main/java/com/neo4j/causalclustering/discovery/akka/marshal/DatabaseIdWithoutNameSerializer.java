/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import org.neo4j.kernel.database.DatabaseId;

public class DatabaseIdWithoutNameSerializer extends BaseAkkaSerializer<DatabaseId>
{
    private static final int sizeHint = 16;

    protected DatabaseIdWithoutNameSerializer()
    {
        super( DatabaseIdWithoutNameMarshal.INSTANCE, DATABASE_ID, sizeHint );
    }
}
