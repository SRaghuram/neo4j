/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.ha.correctness;

import org.junit.Test;

import java.net.URI;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.ProposerMessage;

import static org.junit.Assert.assertEquals;

public class TestProverTimeouts
{

    @Test
    public void equalsShouldBeLogicalAndNotExact() throws Exception
    {
        // Given
        ProverTimeouts timeouts1 = new ProverTimeouts( new URI( "http://asd" ) );
        ProverTimeouts timeouts2 = new ProverTimeouts( new URI( "http://asd" ) );

        timeouts1.setTimeout( "a", Message.internal( ProposerMessage.join ) );
        timeouts1.setTimeout( "b", Message.internal( ProposerMessage.join ) );
        timeouts1.setTimeout( "c", Message.internal( ProposerMessage.join ) );

        timeouts2.setTimeout( "b", Message.internal( ProposerMessage.join ) );
        timeouts2.setTimeout( "c", Message.internal( ProposerMessage.join ) );

        // When
        timeouts1.cancelTimeout( "a" );

        // Then
        assertEquals( timeouts1, timeouts2 );
    }

}
