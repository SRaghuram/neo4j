/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.io.layout;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Objects;

public class DatabaseLayout
{
    private static final File[] EMPTY_FILES_ARRAY = new File[0];
    private final File databaseDirectory;
    private final File databasesDirectory;

    public DatabaseLayout( File databaseDirectory )
    {
        this.databaseDirectory = databaseDirectory;
        this.databasesDirectory = databaseDirectory.getParentFile();
    }

    public DatabaseLayout( File rootDirectory, String databaseName )
    {
        this.databasesDirectory = rootDirectory;
        this.databaseDirectory = new File( rootDirectory, databaseName );
    }

    public File getDatabasesDirectory()
    {
        return databasesDirectory;
    }

    public File databaseDirectory()
    {
        return databaseDirectory;
    }

    public File file( String fileName )
    {
        return new File( databaseDirectory, fileName );
    }

    public File directory( String directoryName )
    {
        return new File( databaseDirectory, directoryName );
    }

    public File[] listDatabaseFiles( FilenameFilter filter )
    {
        File[] files = databaseDirectory.listFiles( filter );
        return files != null ? files : EMPTY_FILES_ARRAY;
    }

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        DatabaseLayout that = (DatabaseLayout) o;
        return Objects.equals( databaseDirectory, that.databaseDirectory ) && Objects.equals( databasesDirectory, that.databasesDirectory );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( databaseDirectory, databasesDirectory );
    }
}
