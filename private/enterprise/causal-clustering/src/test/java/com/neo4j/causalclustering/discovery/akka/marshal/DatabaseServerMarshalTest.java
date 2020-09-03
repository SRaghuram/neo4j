/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.akka.marshal;

import com.neo4j.causalclustering.discovery.akka.database.state.DatabaseServer;
import com.neo4j.causalclustering.identity.IdFactory;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.io.marshal.ChannelMarshal;

import static org.neo4j.kernel.database.TestDatabaseIdRepository.randomDatabaseId;

public class DatabaseServerMarshalTest extends BaseMarshalTest<DatabaseServer>
{
    @Override
    Collection<DatabaseServer> originals()
    {
        return Stream.generate( () -> new DatabaseServer( randomDatabaseId(), IdFactory.randomServerId() ) )
                .limit( 5 )
                .collect( Collectors.toList() );
    }

    @Override
    ChannelMarshal<DatabaseServer> marshal()
    {
        return DatabaseServerMarshal.INSTANCE;
    }
}
