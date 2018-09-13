/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.protocol.handshake;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.neo4j.causalclustering.messaging.Channel;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestModifierProtocols;
import org.neo4j.helpers.collection.Pair;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

/**
 * @see ProtocolHandshakeHappyTest happy path tests
 */
@RunWith( Parameterized.class )
public class ProtocolHandshakeSadTest
{
    @Parameterized.Parameters
    public static Collection<Pair<ApplicationProtocolRepository,ApplicationProtocolRepository>> data()
    {

         ApplicationSupportedProtocols supportsAllRaft = new ApplicationSupportedProtocols( Protocol.ApplicationProtocolCategory.RAFT, emptyList() );
         ApplicationSupportedProtocols supportsAllCatchup = new ApplicationSupportedProtocols( Protocol.ApplicationProtocolCategory.CATCHUP, emptyList() );
         ApplicationSupportedProtocols supportsCatchup1 = new ApplicationSupportedProtocols( Protocol.ApplicationProtocolCategory.CATCHUP, singletonList( 1 ) );
         ApplicationSupportedProtocols supportsCatchup2 = new ApplicationSupportedProtocols( Protocol.ApplicationProtocolCategory.CATCHUP, singletonList( 2 ) );

         ApplicationProtocolRepository raftProtocolsRepository = new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportsAllRaft );
         ApplicationProtocolRepository catchupProtocolsRepository = new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportsAllCatchup );
         ApplicationProtocolRepository catchupV1ProtocolRepository = new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportsCatchup1 );
         ApplicationProtocolRepository catchupV2ProtocolRepository = new ApplicationProtocolRepository( TestApplicationProtocols.values(), supportsCatchup2 );

        return asList(
                Pair.of( catchupProtocolsRepository, raftProtocolsRepository ),
                Pair.of( raftProtocolsRepository, catchupProtocolsRepository ),
                Pair.of( catchupV1ProtocolRepository, catchupV2ProtocolRepository),
                Pair.of( catchupV2ProtocolRepository, catchupV1ProtocolRepository ) );
    }

    @Parameterized.Parameter
    public Pair<ApplicationProtocolRepository,ApplicationProtocolRepository> parameters;

    private HandshakeClient handshakeClient = new HandshakeClient();

    private final Collection<ModifierSupportedProtocols> noModifiers = emptyList();
    private final ModifierProtocolRepository modifierProtocolRepository = new ModifierProtocolRepository( TestModifierProtocols.values(), noModifiers );

    @Test( expected = ClientHandshakeException.class )
    public void shouldFailClientHandshakeOnMismatchedProtocol() throws Throwable
    {
        // given
        HandshakeServer handshakeServer = new HandshakeServer( parameters.first(), modifierProtocolRepository,
                new ProtocolHandshakeHappyTest.FakeServerChannel( handshakeClient ) );
        Channel clientChannel = new ProtocolHandshakeHappyTest.FakeClientChannel( handshakeServer );

        // when
        handshakeClient.initiate( clientChannel, parameters.other(), modifierProtocolRepository );

        // then
        try
        {
            handshakeClient.protocol().getNow( null );
        }
        catch ( CompletionException ex )
        {
            throw ex.getCause();
        }
    }

    @Test( expected = ServerHandshakeException.class )
    public void shouldFailHandshakeForUnknownProtocolOnServer() throws Throwable
    {
        // given
        HandshakeServer handshakeServer = new HandshakeServer( parameters.first(), modifierProtocolRepository,
                new ProtocolHandshakeHappyTest.FakeServerChannel( handshakeClient ) );
        Channel clientChannel = new ProtocolHandshakeHappyTest.FakeClientChannel( handshakeServer );

        // when
        handshakeClient.initiate( clientChannel, parameters.other(), modifierProtocolRepository );
        CompletableFuture<ProtocolStack> serverHandshakeFuture = handshakeServer.protocolStackFuture();

        // then
        try
        {
            serverHandshakeFuture.getNow( null );
        }
        catch ( CompletionException ex )
        {
            throw ex.getCause();
        }
    }
}
