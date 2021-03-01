/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;

import java.util.Collection;
import java.util.List;
import java.util.UUID;

import org.neo4j.io.marshal.ChannelMarshal;
import org.neo4j.kernel.database.DatabaseIdFactory;
import org.neo4j.kernel.database.NamedDatabaseId;

class InefficientNamedDatabaseIdMarshalTest implements BaseMarshalTest<NamedDatabaseId>
{

    @Override
    public Collection<NamedDatabaseId> originals()
    {
        return List.of(
                DatabaseIdFactory.from( "foo", UUID.randomUUID() ),
                DatabaseIdFactory.from( "bar", UUID.randomUUID() ),
                DatabaseIdFactory.from( "", UUID.randomUUID() ) );
    }

    @Override
    public ChannelMarshal<NamedDatabaseId> marshal()
    {
        return InefficientNamedDatabaseIdMarshal.INSTANCE;
    }
}
