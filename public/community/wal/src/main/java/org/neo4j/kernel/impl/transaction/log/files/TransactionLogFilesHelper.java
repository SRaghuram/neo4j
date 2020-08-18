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
package org.neo4j.kernel.impl.transaction.log.files;

import java.io.File;
import java.io.FilenameFilter;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.neo4j.io.fs.FileSystemAbstraction;

import static java.util.regex.Pattern.compile;

public class TransactionLogFilesHelper
{
    public static final String DEFAULT_NAME = "neostore.transaction.db";
    private static final String REGEX_DEFAULT_NAME = "neostore\\.transaction\\.db";
    static final FilenameFilter DEFAULT_FILENAME_FILTER = new LogicalLogFilenameFilter( REGEX_DEFAULT_NAME );

    private static final String VERSION_SUFFIX = ".";
    private static final String REGEX_VERSION_SUFFIX = "\\.";
    private static final Path[] EMPTY_FILES_ARRAY = {};

    private final Path logBaseName;
    private final FilenameFilter logFileFilter;
    private final FileSystemAbstraction fileSystem;

    public TransactionLogFilesHelper( FileSystemAbstraction fileSystem, Path directory )
    {
        this( fileSystem, directory, DEFAULT_NAME );
    }

    public TransactionLogFilesHelper( FileSystemAbstraction fileSystem, Path directory, String name )
    {
        this.fileSystem = fileSystem;
        this.logBaseName = directory.resolve( name );
        this.logFileFilter = new LogicalLogFilenameFilter( name );
    }

    public Path getLogFileForVersion( long version )
    {
        return Path.of( logBaseName.toAbsolutePath().toString() + VERSION_SUFFIX + version );
    }

    public long getLogVersion( Path historyLogFile )
    {
        String historyLogFilename = historyLogFile.getFileName().toString();
        int index = historyLogFilename.lastIndexOf( VERSION_SUFFIX );
        if ( index == -1 )
        {
            throw new RuntimeException( "Invalid log file '" + historyLogFilename + "'" );
        }
        return Long.parseLong( historyLogFilename.substring( index + VERSION_SUFFIX.length() ) );
    }

    FilenameFilter getLogFilenameFilter()
    {
        return logFileFilter;
    }

    public Path[] getLogFiles()
    {
        Path[] files =
                Arrays.stream( fileSystem.listFiles( logBaseName.getParent().toFile(), getLogFilenameFilter() ) ).map( File::toPath ).toArray( Path[]::new );
        if ( files.length == 0 )
        {
            return EMPTY_FILES_ARRAY;
        }
        return files;
    }

    public void accept( LogVersionVisitor visitor )
    {
        for ( Path file : getLogFiles() )
        {
            visitor.visit( file, getLogVersion( file ) );
        }
    }

    private static final class LogicalLogFilenameFilter implements FilenameFilter
    {
        private final Pattern logFilenamePattern;

        LogicalLogFilenameFilter( String name )
        {
            logFilenamePattern = compile( name + REGEX_VERSION_SUFFIX + ".*" );
        }

        @Override
        public boolean accept( File dir, String name )
        {
            return logFilenamePattern.matcher( name ).matches();
        }
    }
}
