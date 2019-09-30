/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import org.neo4j.dbms.DatabaseState;

public class DatabaseStateSerializer extends BaseAkkaSerializer<DatabaseState>
{
    public static int SIZE_HINT = 128;

    public DatabaseStateSerializer()
    {
        super( DatabaseStateMarshal.INSTANCE, DATABASE_STATE, SIZE_HINT );
    }
}
