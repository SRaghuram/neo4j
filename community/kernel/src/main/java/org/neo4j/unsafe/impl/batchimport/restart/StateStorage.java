/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.restart;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.FlushableChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableClosableChannel;

import static org.neo4j.kernel.impl.store.PropertyType.EMPTY_BYTE_ARRAY;
import static org.neo4j.unsafe.impl.batchimport.restart.ChannelUtils.readString;
import static org.neo4j.unsafe.impl.batchimport.restart.ChannelUtils.writeString;

public class StateStorage
{
    public static final String NO_STATE = "";

    private final FileSystemAbstraction fs;
    private final File stateFile;

    public StateStorage( FileSystemAbstraction fs, File stateFile )
    {
        this.fs = fs;
        this.stateFile = stateFile;
    }

    public Pair<String,byte[]> get() throws IOException
    {
        try ( ReadableClosableChannel channel = new ReadAheadChannel<>( fs.open( stateFile, "r" ) ) )
        {
            String name = readString( channel );
            byte[] checkPoint = new byte[channel.getInt()];
            channel.get( checkPoint, checkPoint.length );
            return Pair.of( name, checkPoint );
        }
        catch ( FileNotFoundException e )
        {
            return Pair.of( NO_STATE, EMPTY_BYTE_ARRAY );
        }
    }

    public void set( String name, byte[] checkPoint ) throws IOException
    {
        fs.truncate( stateFile, 0 );
        try ( FlushableChannel channel = new PhysicalFlushableChannel( fs.open( stateFile, "rw" ) ) )
        {
            writeString( name, channel );
            channel.putInt( checkPoint.length );
            channel.put( checkPoint, checkPoint.length );
        }
    }

    public void remove() throws IOException
    {
        fs.deleteFileOrThrow( stateFile );
    }
}
