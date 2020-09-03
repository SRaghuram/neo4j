/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.akka.database.state.DatabaseServer;

public class DatabaseServerSerializer extends BaseAkkaSerializer<DatabaseServer>
{
    private static final int SIZE_HINT = 96;

    public DatabaseServerSerializer()
    {
        super( DatabaseServerMarshal.INSTANCE, DATABASE_SERVER, SIZE_HINT );
    }
}
