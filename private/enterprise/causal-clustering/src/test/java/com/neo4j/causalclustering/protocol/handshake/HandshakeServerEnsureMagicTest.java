/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.handshake;

import com.neo4j.causalclustering.messaging.Channel;
import com.neo4j.causalclustering.protocol.Protocol;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.concurrent.CompletionException;

import org.neo4j.helpers.collection.Iterators;

import static com.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.RAFT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

@RunWith( Parameterized.class )
public class HandshakeServerEnsureMagicTest
{
    @Parameterized.Parameters( name = "{0}" )
    public static Collection<ServerMessage> data()
    {
        return asList(
                new ApplicationProtocolRequest( RAFT.canonicalName(), Iterators.asSet( 1, 2 ) ),
                new ModifierProtocolRequest( Protocol.ModifierProtocolCategory.COMPRESSION.canonicalName(), Iterators.asSet( "3", "4" ) ),
                new SwitchOverRequest( RAFT.canonicalName(), 2, emptyList() )
        );
    }

    @Parameterized.Parameter
    public ServerMessage message;

    private final ApplicationSupportedProtocols supportedApplicationProtocol =
            new ApplicationSupportedProtocols( RAFT, TestProtocols.TestApplicationProtocols.listVersionsOf( RAFT ) );

    private Channel channel = mock( Channel.class );
    private ApplicationProtocolRepository applicationProtocolRepository =
            new ApplicationProtocolRepository( TestProtocols.TestApplicationProtocols.values(), supportedApplicationProtocol );
    private ModifierProtocolRepository modifierProtocolRepository =
            new ModifierProtocolRepository( TestProtocols.TestModifierProtocols.values(), emptyList() );

    private HandshakeServer server = new HandshakeServer(
            applicationProtocolRepository,
            modifierProtocolRepository, channel );

    @Test( expected = IllegalStateException.class )
    public void shouldThrowIfMagicHasNotBeenSent()
    {
        message.dispatch( server );
    }

    @Test( expected = ServerHandshakeException.class )
    public void shouldCompleteExceptionallyIfMagicHasNotBeenSent() throws Throwable
    {
        // when
        try
        {
            message.dispatch( server );
        }
        catch ( Exception ignored )
        {
            // swallow, tested elsewhere
        }

        // then future is completed exceptionally
        try
        {
            server.protocolStackFuture().getNow( null );
        }
        catch ( CompletionException completion )
        {
            throw completion.getCause();
        }
    }

    @Test
    public void shouldNotThrowIfMagicHasBeenSent()
    {
        // given
        InitialMagicMessage.instance().dispatch( server );

        // when
        message.dispatch( server );

        // then pass
    }

    @Test
    public void shouldNotCompleteExceptionallyIfMagicHasBeenSent()
    {
        // given
        InitialMagicMessage.instance().dispatch( server );

        // when
        message.dispatch( server );

        // then future should either not complete exceptionally or do so for non-magic reasons
        try
        {
            server.protocolStackFuture().getNow( null );
        }
        catch ( CompletionException ex )
        {
            assertThat( ex.getMessage().toLowerCase(), Matchers.not( Matchers.containsString( "magic" ) ) );
        }
    }
}
