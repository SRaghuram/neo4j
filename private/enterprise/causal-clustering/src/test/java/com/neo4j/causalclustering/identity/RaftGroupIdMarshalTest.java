/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.identity;

import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.io.marshal.ChannelMarshal;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.neo4j.kernel.database.DatabaseIdRepository.NAMED_SYSTEM_DATABASE_ID;

class RaftGroupIdMarshalTest implements BaseMarshalTest<RaftGroupId>
{

    @Override
    public ChannelMarshal<RaftGroupId> marshal()
    {
        return RaftGroupId.Marshal.INSTANCE;
    }

    @Override
    public Collection<RaftGroupId> originals()
    {
        var others =  Stream.generate( IdFactory::randomRaftId ).limit( 5 );
        var system = Stream.of( RaftGroupId.from( NAMED_SYSTEM_DATABASE_ID.databaseId() ) );

        return Stream.concat( others, system )
                     .collect( Collectors.toList() );
    }

    @Test
    void shouldMarshalNullRaftId() throws Throwable
    {
        // given/when
        var result = marshalAndUnmarshal( null, marshal() );

        // then
        assertNull( result );
    }
}
