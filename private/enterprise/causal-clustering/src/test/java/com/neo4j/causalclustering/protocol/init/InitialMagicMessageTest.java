/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.protocol.init;

import org.junit.jupiter.api.Test;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class InitialMagicMessageTest
{
    @Test
    void shouldCreateWithCorrectMagicValue()
    {
        // given
        InitialMagicMessage magicMessage = InitialMagicMessage.instance();

        // then
        assertTrue( magicMessage.isCorrectMagic() );
        assertEquals( "NEO4J_CLUSTER", magicMessage.magic() );
    }

    @Test
    void shouldHaveCorrectMessageCode()
    {
        byte[] bytes = InitialMagicMessage.CORRECT_MAGIC_VALUE.substring( 0, 4 ).getBytes( UTF_8 );
        int messageCode = bytes[0] | (bytes[1] << 8) | (bytes[2] << 16) | (bytes[3] << 24);

        assertEquals( 0x344F454E, messageCode );
        assertEquals( 0x344F454E, InitialMagicMessage.MESSAGE_CODE );
    }
}
