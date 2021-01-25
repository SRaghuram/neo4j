/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol;

import com.neo4j.causalclustering.core.consensus.protocol.RaftProtocolClientInstaller;
import com.neo4j.causalclustering.core.consensus.protocol.RaftProtocolServerInstaller;
import com.neo4j.causalclustering.messaging.marshalling.v2.SupportedMessagesV2;
import com.neo4j.causalclustering.messaging.marshalling.DecodingDispatcher;
import com.neo4j.causalclustering.messaging.marshalling.RaftMessageComposer;
import com.neo4j.causalclustering.messaging.marshalling.RaftMessageDecoder;
import com.neo4j.causalclustering.messaging.marshalling.RaftMessageEncoder;
import com.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation;
import com.neo4j.causalclustering.protocol.application.ApplicationProtocols;
import com.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import com.neo4j.causalclustering.protocol.handshake.TestProtocols;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocol;
import com.neo4j.causalclustering.protocol.modifier.ModifierProtocols;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.util.Collection;
import java.util.List;

import org.neo4j.logging.NullLogProvider;

import static com.neo4j.causalclustering.protocol.application.ApplicationProtocols.RAFT_2_0;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtocolInstallerRepositoryTest
{
    private final List<ModifierProtocolInstaller<Orientation.Client>> clientModifiers =
            asList( new SnappyClientInstaller(),
                    new LZOClientInstaller(),
                    new LZ4ClientInstaller(),
                    new LZ4HighCompressionClientInstaller(),
                    new Rot13ClientInstaller() );
    private final List<ModifierProtocolInstaller<Orientation.Server>> serverModifiers =
            asList( new SnappyServerInstaller(),
                    new LZOServerInstaller(),
                    new LZ4ServerInstaller(),
                    new LZ4ValidatingServerInstaller(),
                    new Rot13ServerInstaller() );

    private final NettyPipelineBuilderFactory pipelineBuilderFactory =
            NettyPipelineBuilderFactory.insecure();
    RaftProtocolClientInstaller.Factory raftProtocolClientInstaller =
            new RaftProtocolClientInstaller.Factory( pipelineBuilderFactory,
                                                     NullLogProvider.getInstance(),
                                                     new SupportedMessagesV2(),
                                                     () -> new RaftMessageEncoder(),
                                                     ApplicationProtocols.RAFT_2_0 );

    RaftProtocolServerInstaller.Factory raftProtocolServerInstaller =
            new RaftProtocolServerInstaller.Factory( null,
                                                     pipelineBuilderFactory,
                                                     NullLogProvider.getInstance(),
                                                     c -> new DecodingDispatcher( c, NullLogProvider.getInstance(), RaftMessageDecoder::new ),
                                                     () -> new RaftMessageComposer( Clock.systemUTC() ),
                                                     RAFT_2_0 );
    private final ProtocolInstallerRepository<Orientation.Client> clientRepository =
            new ProtocolInstallerRepository<>( List.of( raftProtocolClientInstaller ), clientModifiers );
    private final ProtocolInstallerRepository<Orientation.Server> serverRepository =
            new ProtocolInstallerRepository<>( List.of( raftProtocolServerInstaller ), serverModifiers );

    @Test
    void shouldReturnModifierProtocolsForClient()
    {
        // given
        ModifierProtocol expected = TestProtocols.TestModifierProtocols.SNAPPY;
        ProtocolStack protocolStack = new ProtocolStack( ApplicationProtocols.RAFT_2_0, List.of( expected ) );

        // when
        Collection<Collection<ModifierProtocol>> actual = clientRepository.installerFor( protocolStack ).modifiers();

        // then
        assertThat( actual, contains( contains( expected ) ) );
    }

    @Test
    void shouldReturnModifierProtocolsForServer()
    {
        // given
        ModifierProtocol expected = TestProtocols.TestModifierProtocols.SNAPPY;
        ProtocolStack protocolStack = new ProtocolStack( ApplicationProtocols.RAFT_2_0, List.of( expected ) );

        // when
        Collection<Collection<ModifierProtocol>> actual = serverRepository.installerFor( protocolStack ).modifiers();

        // then
        assertThat( actual, contains( contains( expected ) ) );
    }

    @Test
    void shouldReturnModifierProtocolsForProtocolWithSharedInstallerForClient()
    {
        // given
        ModifierProtocol expected = TestProtocols.TestModifierProtocols.LZ4_HIGH_COMPRESSION_VALIDATING;
        TestProtocols.TestModifierProtocols alsoSupported = TestProtocols.TestModifierProtocols.LZ4_HIGH_COMPRESSION;

        ProtocolStack protocolStack = new ProtocolStack( ApplicationProtocols.RAFT_2_0, List.of( expected ) );

        // when
        Collection<Collection<ModifierProtocol>> actual = clientRepository.installerFor( protocolStack ).modifiers();

        // then
        assertThat( actual, contains( containsInAnyOrder( expected, alsoSupported ) )) ;
    }

    @Test
    void shouldReturnModifierProtocolsForProtocolWithSharedInstallerForServer()
    {
        // given
        ModifierProtocol expected = TestProtocols.TestModifierProtocols.LZ4_HIGH_COMPRESSION_VALIDATING;
        TestProtocols.TestModifierProtocols alsoSupported = TestProtocols.TestModifierProtocols.LZ4_VALIDATING;

        ProtocolStack protocolStack = new ProtocolStack( ApplicationProtocols.RAFT_2_0, List.of( expected ) );

        // when
        Collection<Collection<ModifierProtocol>> actual = serverRepository.installerFor( protocolStack ).modifiers();

        // then
        assertThat( actual, contains( containsInAnyOrder( expected, alsoSupported ) )) ;
    }

    @Test
    void shouldUseDifferentInstancesOfProtocolInstaller()
    {
        // given
        ProtocolStack protocolStack1 = new ProtocolStack( ApplicationProtocols.RAFT_2_0, List.of( TestProtocols.TestModifierProtocols.SNAPPY ) );
        ProtocolStack protocolStack2 = new ProtocolStack( ApplicationProtocols.RAFT_2_0, List.of( TestProtocols.TestModifierProtocols.LZO ) );

        // when
        ProtocolInstaller<Orientation.Client> protocolInstaller1 = clientRepository.installerFor( protocolStack1 );
        ProtocolInstaller<Orientation.Client> protocolInstaller2 = clientRepository.installerFor( protocolStack2 );

        // then
        assertThat( protocolInstaller1, not( sameInstance( protocolInstaller2 ) ) );
    }

    @Test
    void shouldThrowIfAttemptingToCreateInstallerForMultipleModifiersWithSameIdentifier()
    {
        // given
        ProtocolStack protocolStack = new ProtocolStack(
                ApplicationProtocols.RAFT_2_0,
                asList( TestProtocols.TestModifierProtocols.SNAPPY, TestProtocols.TestModifierProtocols.LZO ) );

        // then
        assertThrows( IllegalArgumentException.class, () -> clientRepository.installerFor( protocolStack ) );
    }

    @Test
    void shouldNotInitialiseIfMultipleInstallersForSameProtocolForServer()
    {
        assertThrows( IllegalArgumentException.class,
                () -> new ProtocolInstallerRepository<>( asList( raftProtocolServerInstaller, raftProtocolServerInstaller ), emptyList() ) );
    }

    @Test
    void shouldNotInitialiseIfMultipleInstallersForSameProtocolForClient()
    {
        assertThrows( IllegalArgumentException.class,
                () -> new ProtocolInstallerRepository<>( asList( raftProtocolClientInstaller, raftProtocolClientInstaller ), emptyList() ) );
    }

    @Test
    void shouldThrowIfUnknownProtocolForServer()
    {
        assertThrows( IllegalStateException.class,
                () -> serverRepository.installerFor( new ProtocolStack( TestProtocols.TestApplicationProtocols.RAFT_3, emptyList() ) ) );
    }

    @Test
    void shouldThrowIfUnknownProtocolForClient()
    {
        assertThrows( IllegalStateException.class,
                () -> clientRepository.installerFor( new ProtocolStack( TestProtocols.TestApplicationProtocols.RAFT_3, emptyList() ) ) );
    }

    @Test
    void shouldThrowIfUnknownModifierProtocol()
    {
        // given
        // setup used TestModifierProtocols, doesn't know about production protocols
        ModifierProtocol unknownProtocol = ModifierProtocols.COMPRESSION_SNAPPY;

        // then
        assertThrows( IllegalStateException.class,
                () -> serverRepository.installerFor( new ProtocolStack( ApplicationProtocols.RAFT_2_0, List.of( unknownProtocol ) ) ) );
    }

    // Dummy installers

    private static class SnappyClientInstaller extends ModifierProtocolInstaller.BaseClientModifier
    {
        private SnappyClientInstaller()
        {
            super( "snappy", null, TestProtocols.TestModifierProtocols.SNAPPY );
        }
    }

    private static class LZOClientInstaller extends ModifierProtocolInstaller.BaseClientModifier
    {
        private LZOClientInstaller()
        {
            super( "lzo", null, TestProtocols.TestModifierProtocols.LZO );
        }
    }

    private static class LZ4ClientInstaller extends ModifierProtocolInstaller.BaseClientModifier
    {
        private LZ4ClientInstaller()
        {
            super( "lz4", null, TestProtocols.TestModifierProtocols.LZ4, TestProtocols.TestModifierProtocols.LZ4_VALIDATING );
        }
    }
    private static class LZ4HighCompressionClientInstaller extends ModifierProtocolInstaller.BaseClientModifier
    {
        private LZ4HighCompressionClientInstaller()
        {
            super( "lz4", null, TestProtocols.TestModifierProtocols.LZ4_HIGH_COMPRESSION, TestProtocols.TestModifierProtocols.LZ4_HIGH_COMPRESSION_VALIDATING );
        }
    }

    private class Rot13ClientInstaller extends ModifierProtocolInstaller.BaseClientModifier
    {
        Rot13ClientInstaller()
        {
            super( "rot13", null, TestProtocols.TestModifierProtocols.ROT13 );
        }
    }

    private static class SnappyServerInstaller extends ModifierProtocolInstaller.BaseServerModifier
    {
        private SnappyServerInstaller()
        {
            super( "snappy", null, TestProtocols.TestModifierProtocols.SNAPPY );
        }
    }

    private static class LZOServerInstaller extends ModifierProtocolInstaller.BaseServerModifier
    {
        private LZOServerInstaller()
        {
            super( "lzo", null, TestProtocols.TestModifierProtocols.LZO );
        }
    }

    private static class LZ4ServerInstaller extends ModifierProtocolInstaller.BaseServerModifier
    {
        private LZ4ServerInstaller()
        {
            super( "lz4", null, TestProtocols.TestModifierProtocols.LZ4, TestProtocols.TestModifierProtocols.LZ4_HIGH_COMPRESSION );
        }
    }

    private static class LZ4ValidatingServerInstaller extends ModifierProtocolInstaller.BaseServerModifier
    {
        private LZ4ValidatingServerInstaller()
        {
            super( "lz4", null, TestProtocols.TestModifierProtocols.LZ4_VALIDATING, TestProtocols.TestModifierProtocols.LZ4_HIGH_COMPRESSION_VALIDATING );
        }
    }

    private class Rot13ServerInstaller extends ModifierProtocolInstaller.BaseServerModifier
    {
        Rot13ServerInstaller()
        {
            super( "rot13", null, TestProtocols.TestModifierProtocols.ROT13 );
        }
    }
}
