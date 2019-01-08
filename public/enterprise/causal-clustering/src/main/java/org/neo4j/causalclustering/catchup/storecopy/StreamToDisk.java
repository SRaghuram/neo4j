/*
 * Copyright (c) 2002-2019 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.neo4j.causalclustering.catchup.tx.FileCopyMonitor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.monitoring.Monitors;

class StreamToDisk implements StoreFileStreams
{
    private final File storeDir;
    private final FileSystemAbstraction fs;
    private final FileCopyMonitor fileCopyMonitor;

    StreamToDisk( File storeDir, FileSystemAbstraction fs, Monitors monitors ) throws IOException
    {
        this.storeDir = storeDir;
        this.fs = fs;
        fs.mkdirs( storeDir );
        this.fileCopyMonitor = monitors.newMonitor( FileCopyMonitor.class );
    }

    @Override
    public OutputStream createStream( String destination ) throws IOException
    {
        File fileName = new File( storeDir, destination );
        fs.mkdirs( fileName.getParentFile() );

        fileCopyMonitor.copyFile( fileName );

        return fs.openAsOutputStream( fileName, true );
    }
}
