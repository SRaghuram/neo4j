/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery.procedures;

import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import com.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Stream;

import org.neo4j.collection.RawIterator;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.internal.helpers.collection.Pair;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.internal.kernel.api.procs.QualifiedName;
import org.neo4j.values.AnyValue;

import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayContaining;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.values.storable.Values.stringValue;

public class InstalledProtocolsProcedureTest
{
    private final Pair<SocketAddress,ProtocolStack> outbound1 =
            Pair.of( new SocketAddress( "host1", 1 ),
                    new ProtocolStack( TestApplicationProtocols.RAFT_1, List.of( TestModifierProtocols.SNAPPY ) ) );
    private final Pair<SocketAddress,ProtocolStack> outbound2 =
            Pair.of( new SocketAddress( "host2", 2 ),
                    new ProtocolStack( TestApplicationProtocols.RAFT_2, List.of( TestModifierProtocols.SNAPPY, TestModifierProtocols.ROT13 ) ) );

    private final Pair<SocketAddress,ProtocolStack> inbound1 =
            Pair.of( new SocketAddress( "host3", 3 ),
                    new ProtocolStack( TestApplicationProtocols.RAFT_3, List.of( TestModifierProtocols.SNAPPY ) ) );
    private final Pair<SocketAddress,ProtocolStack> inbound2 =
            Pair.of( new SocketAddress( "host4", 4 ),
                    new ProtocolStack( TestApplicationProtocols.RAFT_4, emptyList() ) );

    @Test
    void shouldHaveEmptyOutputIfNoInstalledProtocols() throws Throwable
    {
        // given
        InstalledProtocolsProcedure installedProtocolsProcedure =
                new InstalledProtocolsProcedure( Stream::empty, Stream::empty );

        // when
        RawIterator<AnyValue[],ProcedureException> result = installedProtocolsProcedure.apply( null, null, null );

        // then
        assertFalse( result.hasNext() );
    }

    @Test
    void shouldListOutboundProtocols() throws Throwable
    {
        // given
        InstalledProtocolsProcedure installedProtocolsProcedure =
                new InstalledProtocolsProcedure( () -> Stream.of( outbound1, outbound2 ), Stream::empty );

        // when
        RawIterator<AnyValue[],ProcedureException> result = installedProtocolsProcedure.apply( null, null, null );

        // then
        assertThat( result.next(),
                arrayContaining( stringValue( "outbound" ), stringValue( "host1:1" ), stringValue( "raft" ),
                        stringValue( "1.0" ), stringValue( "[TestSnappy]" ) ) );
        assertThat( result.next(),
                arrayContaining( stringValue( "outbound" ), stringValue( "host2:2" ), stringValue( "raft" ),
                        stringValue( "2.0" ), stringValue( "[TestSnappy,ROT13]" ) ) );
        assertFalse( result.hasNext() );
    }

    @Test
    void shouldListInboundProtocols() throws Throwable
    {
        // given
        InstalledProtocolsProcedure installedProtocolsProcedure =
                new InstalledProtocolsProcedure( Stream::empty, () -> Stream.of( inbound1, inbound2 ) );

        // when
        RawIterator<AnyValue[],ProcedureException> result = installedProtocolsProcedure.apply( null, null, null );

        // then
        assertThat( result.next(),
                arrayContaining( stringValue( "inbound" ), stringValue( "host3:3" ), stringValue( "raft" ),
                        stringValue( "3.0" ), stringValue( "[TestSnappy]" ) ) );
        assertThat( result.next(),
                arrayContaining( stringValue( "inbound" ), stringValue( "host4:4" ), stringValue( "raft" ),
                        stringValue( "4.0" ), stringValue( "[]" ) ) );
        assertFalse( result.hasNext() );
    }

    @Test
    void shouldListInboundAndOutboundProtocols() throws Throwable
    {
        // given
        InstalledProtocolsProcedure installedProtocolsProcedure =
                new InstalledProtocolsProcedure( () -> Stream.of( outbound1, outbound2 ), () -> Stream.of( inbound1, inbound2 ) );

        // when
        RawIterator<AnyValue[],ProcedureException> result = installedProtocolsProcedure.apply( null, null, null );

        // then
        assertThat( result.next(),
                arrayContaining( stringValue( "outbound" ), stringValue( "host1:1" ), stringValue( "raft" ),
                        stringValue( "1.0" ), stringValue( "[TestSnappy]" ) ) );
        assertThat( result.next(),
                arrayContaining( stringValue( "outbound" ), stringValue( "host2:2" ), stringValue( "raft" ),
                        stringValue( "2.0" ), stringValue( "[TestSnappy,ROT13]" ) ) );
        assertThat( result.next(),
                arrayContaining( stringValue( "inbound" ), stringValue( "host3:3" ), stringValue( "raft" ),
                        stringValue( "3.0" ), stringValue( "[TestSnappy]" ) ) );
        assertThat( result.next(),
                arrayContaining( stringValue( "inbound" ), stringValue( "host4:4" ), stringValue( "raft" ),
                        stringValue( "4.0" ), stringValue( "[]" ) ) );
        assertFalse( result.hasNext() );
    }

    @Test
    void shouldHaveCorrectName()
    {
        var procedure = new InstalledProtocolsProcedure( Stream::empty, Stream::empty );

        assertEquals( new QualifiedName( List.of( "dbms", "cluster" ), "protocols" ), procedure.signature().name() );
    }

    @Test
    void shouldBeASystemProcedure()
    {
        var procedure = new InstalledProtocolsProcedure( Stream::empty, Stream::empty );

        assertTrue( procedure.signature().systemProcedure() );
    }
}
