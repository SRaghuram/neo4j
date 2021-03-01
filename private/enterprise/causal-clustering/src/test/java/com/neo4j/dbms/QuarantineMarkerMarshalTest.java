/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.io.marshal.ChannelMarshal;

class QuarantineMarkerMarshalTest implements BaseMarshalTest<QuarantineMarker>
{
    @Override
    public Collection<QuarantineMarker> originals()
    {
        return Stream.of( "foo", "bar", "", "987afd_*!" )
              .map( QuarantineMarker::new )
              .collect( Collectors.toList() );
    }

    @Override
    public ChannelMarshal<QuarantineMarker> marshal()
    {
        return QuarantineMarker.Marshal.INSTANCE;
    }
}
