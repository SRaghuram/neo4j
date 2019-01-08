/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.messaging.marshalling;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;

public class StringMarshalTest
{
    @Test
    public void shouldSerializeAndDeserializeString()
    {
        // given
        final String TEST_STRING = "ABC123_?";
        final ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.buffer();

        // when
        StringMarshal.marshal( buffer, TEST_STRING );
        String reconstructed = StringMarshal.unmarshal( buffer );

        // then
        assertNotSame( TEST_STRING, reconstructed );
        assertEquals( TEST_STRING, reconstructed );
    }

    @Test
    public void shouldSerializeAndDeserializeEmptyString()
    {
        // given
        final String TEST_STRING = "";
        final ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.buffer();

        // when
        StringMarshal.marshal( buffer, TEST_STRING );
        String reconstructed = StringMarshal.unmarshal( buffer );

        // then
        assertNotSame( TEST_STRING, reconstructed );
        assertEquals( TEST_STRING, reconstructed );
    }

    @Test
    public void shouldSerializeAndDeserializeNull() throws Exception
    {
        // given
        final ByteBuf buffer = UnpooledByteBufAllocator.DEFAULT.buffer();

        // when
        StringMarshal.marshal( buffer, null );
        String reconstructed = StringMarshal.unmarshal( buffer );

        // then
        assertNull( reconstructed );
    }
}
