/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.recordstorage;

import org.junit.jupiter.api.Test;

import org.neo4j.internal.recordstorage.legacy.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.transaction.log.InMemoryClosableChannel;

import static org.junit.jupiter.api.Assertions.assertEquals;

class PhysicalLogNeoCommandReaderTest
{
    @Test
    void shouldReadIndexCommandHeaderCorrectly() throws Exception
    {
        // This bug manifested in header byte[1] {0,1,2}, which contains:
        // [x   ,    ] start node needs long
        // [ x  ,    ] end node needs long
        // [  xx,xxxx] index name id
        // would have the mask for reading "start node needs long" to 0x8, where it should have been 0x80.
        // So we need an index name id which has the 0x8 bit set to falsely read that value as "true".
        // Number 12 will do just fine.

        // GIVEN
        PhysicalLogCommandReaderV3_0_10 reader = new PhysicalLogCommandReaderV3_0_10();
        InMemoryClosableChannel data = new InMemoryClosableChannel();
        AddRelationshipCommand command = new AddRelationshipCommand();
        byte indexNameId = (byte)12;
        long entityId = 123;
        byte keyId = (byte)1;
        Object value = "test value";
        long startNode = 14;
        long endNode = 15;

        // WHEN
        command.init( indexNameId, entityId, keyId, value, startNode, endNode );
        command.serialize( data );

        // THEN
        AddRelationshipCommand readCommand = (AddRelationshipCommand) reader.read( data );
        assertEquals( indexNameId, readCommand.getIndexNameId() );
        assertEquals( entityId, readCommand.getEntityId() );
        assertEquals( keyId, readCommand.getKeyId() );
        assertEquals( value, readCommand.getValue() );
        assertEquals( startNode, readCommand.getStartNode() );
        assertEquals( endNode, readCommand.getEndNode() );
    }

    @Test
    void shouldProperlyMaskIndexIdFieldInIndexHeader() throws Exception
    {
        /* This is how the index command header is laid out
         * [x   ,    ] start node needs long
         * [ x  ,    ] end node needs long
         * [  xx,xxxx] index name id
         * This means that the index name id can be in the range of 0 to 63. This test verifies that
         * this constraint is actually respected
         */

        // GIVEN
        PhysicalLogCommandReaderV3_0_10 reader = new PhysicalLogCommandReaderV3_0_10();
        InMemoryClosableChannel data = new InMemoryClosableChannel();
        // Here we take advantage of the fact that all index commands have the same header written out
        AddRelationshipCommand command = new AddRelationshipCommand();
        long entityId = 123;
        byte keyId = (byte)1;
        Object value = "test value";
        long startNode = 14;
        long endNode = 15;

        for ( byte indexByteId = 0; indexByteId < 63; indexByteId++ )
        {
            // WHEN
            command.init( indexByteId, entityId, keyId, value, startNode, endNode );
            command.serialize( data );

            // THEN
            AddRelationshipCommand readCommand = (AddRelationshipCommand) reader.read( data );
            assertEquals( indexByteId, readCommand.getIndexNameId() );
            assertEquals( entityId, readCommand.getEntityId() );
            assertEquals( keyId, readCommand.getKeyId() );
            assertEquals( value, readCommand.getValue() );
            assertEquals( startNode, readCommand.getStartNode() );
            assertEquals( endNode, readCommand.getEndNode() );

            data.reset();
        }
    }
}
