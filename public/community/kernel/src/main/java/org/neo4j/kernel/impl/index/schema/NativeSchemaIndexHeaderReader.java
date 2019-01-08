/*
 * Copyright (c) 2002-2019 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.index.schema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import org.neo4j.index.internal.gbptree.Header;

import static org.neo4j.kernel.impl.index.schema.NativeSchemaNumberIndexPopulator.BYTE_FAILED;

class NativeSchemaIndexHeaderReader implements Header.Reader
{
    byte state;
    String failureMessage;

    @Override
    public void read( ByteBuffer headerData )
    {
        state = headerData.get();
        if ( state == BYTE_FAILED )
        {
            short messageLength = headerData.getShort();
            byte[] failureMessageBytes = new byte[messageLength];
            headerData.get( failureMessageBytes );
            failureMessage = new String( failureMessageBytes, StandardCharsets.UTF_8 );
        }
    }
}
