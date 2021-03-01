/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.version;

import com.neo4j.causalclustering.test_helpers.BaseMarshalTest;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.io.marshal.ChannelMarshal;

import static java.lang.Integer.parseInt;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ClusterStateVersionMarshalTest implements BaseMarshalTest<ClusterStateVersion>
{
    @Override
    public ChannelMarshal<ClusterStateVersion> marshal()
    {
        return new ClusterStateVersionMarshal();
    }

    @Override
    public Collection<ClusterStateVersion> originals()
    {
        return Stream.of( "0.0", "0.42", "1.0", "1.3", "42.0", "42.42", "9.99" )
                .map( ClusterStateVersionMarshalTest::newVersion )
                .collect( Collectors.toList() );
    }

    @Test
    void shouldHaveNullStartState()
    {
        var marshal = new ClusterStateVersionMarshal();

        assertNull( marshal.startState() );
    }

    @Test
    void shouldNotSupportOrdinal()
    {
        var marshal = new ClusterStateVersionMarshal();

        assertThrows( UnsupportedOperationException.class, () -> marshal.ordinal( new ClusterStateVersion( 1, 0 ) ) );
    }

    private static ClusterStateVersion newVersion( String versionString )
    {
        var split = versionString.split( "\\." );
        return new ClusterStateVersion( parseInt( split[0] ), parseInt( split[1] ) );
    }
}
