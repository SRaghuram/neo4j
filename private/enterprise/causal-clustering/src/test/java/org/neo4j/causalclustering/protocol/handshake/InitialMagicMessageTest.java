/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.causalclustering.protocol.handshake;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InitialMagicMessageTest
{
    @Test
    public void shouldCreateWithCorrectMagicValue()
    {
        // given
        InitialMagicMessage magicMessage = InitialMagicMessage.instance();

        // then
        assertTrue( magicMessage.isCorrectMagic() );
        assertEquals( "NEO4J_CLUSTER", magicMessage.magic() );
    }

    @Test
    public void shouldHaveCorrectMessageCode() throws Exception
    {
        byte[] bytes = InitialMagicMessage.CORRECT_MAGIC_VALUE.substring( 0, 4 ).getBytes( "UTF-8" );
        int messageCode = bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24);

        assertEquals( 0x344F454E, messageCode );
        assertEquals( 0x344F454E, InitialMagicMessage.MESSAGE_CODE );
    }
}
