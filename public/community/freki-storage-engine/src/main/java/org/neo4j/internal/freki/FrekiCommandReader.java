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
package org.neo4j.internal.freki;

import java.io.IOException;

import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.storageengine.api.CommandReader;
import org.neo4j.storageengine.api.StorageCommand;

class FrekiCommandReader implements CommandReader
{
    static final FrekiCommandReader INSTANCE = new FrekiCommandReader();

    private FrekiCommandReader()
    {
    }

    @Override
    public StorageCommand read( ReadableChannel channel ) throws IOException
    {
        byte commandType;
        do
        {
            commandType = channel.get();
        }
        while ( commandType == CommandReader.NONE );

        return read( commandType, channel );
    }

    private StorageCommand read( byte commandType, ReadableChannel channel ) throws IOException
    {
        //Type already
        switch ( commandType )
        {
        case FrekiCommand.SparseNode.TYPE :
            return FrekiCommand.SparseNode.deserialize( channel );
        case FrekiCommand.DenseNode.TYPE :
            return FrekiCommand.DenseNode.deserialize( channel );
        case FrekiCommand.BigPropertyValue.TYPE :
            return FrekiCommand.BigPropertyValue.deserialize( channel );
        case FrekiCommand.LabelToken.TYPE :
            return FrekiCommand.LabelToken.deserialize( channel );
        case FrekiCommand.RelationshipTypeToken.TYPE :
            return FrekiCommand.RelationshipTypeToken.deserialize( channel );
        case FrekiCommand.PropertyKeyToken.TYPE :
            return FrekiCommand.PropertyKeyToken.deserialize( channel );
        case FrekiCommand.Schema.TYPE :
            return FrekiCommand.Schema.deserialize( channel );
        case FrekiCommand.NodeCount.TYPE :
            return FrekiCommand.NodeCount.deserialize( channel );
        case FrekiCommand.RelationshipCount.TYPE :
            return FrekiCommand.RelationshipCount.deserialize( channel );
        default: throw new UnsupportedOperationException( String.format( "Command %d not implemented", commandType ) );
        }
    }
}
