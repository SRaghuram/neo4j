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

import org.apache.commons.codec.Charsets;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Writer;
import org.neo4j.io.fs.FileSystemAbstraction;

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

    public String get() throws IOException
    {
        try ( BufferedReader reader = new BufferedReader( fs.openAsReader( stateFile, Charsets.UTF_8 ) ) )
        {
            return reader.readLine();
        }
        catch ( FileNotFoundException e )
        {
            return NO_STATE;
        }
    }

    public void set( String name ) throws IOException
    {
        fs.truncate( stateFile, 0 );
        try ( Writer writer = fs.openAsWriter( stateFile, Charsets.UTF_8, false ) )
        {
            writer.write( name );
        }
    }

    public void remove() throws IOException
    {
        fs.deleteFileOrThrow( stateFile );
    }
}
