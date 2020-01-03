/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.akka.database.state.DatabaseToMember;

public class DatabaseToMemberSerializer extends BaseAkkaSerializer<DatabaseToMember>
{
    private static final int SIZE_HINT = 96;

    public DatabaseToMemberSerializer()
    {
        super( DatabaseToMemberMarshal.INSTANCE, DATABASE_TO_MEMBER, SIZE_HINT );
    }
}
