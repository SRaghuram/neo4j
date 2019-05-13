/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.consensus.membership;

import com.neo4j.causalclustering.discovery.akka.marshal.BaseMarshalTest;
import com.neo4j.causalclustering.messaging.marshalling.DatabaseIdMarshal;

import org.neo4j.kernel.database.DatabaseId;
import org.neo4j.kernel.database.TestDatabaseIdRepository;

public class DatabaseIdMarshalTest extends BaseMarshalTest<DatabaseId>
{
    public DatabaseIdMarshalTest()
    {
        super( new TestDatabaseIdRepository().get( "database name" ), DatabaseIdMarshal.INSTANCE );
    }
}
