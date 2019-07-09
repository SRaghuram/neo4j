/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.v4.messaging;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.runtime.Bookmark;
import org.neo4j.bolt.v4.runtime.bookmarking.BookmarkWithDatabaseId;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.parseDatabaseName;

public class RunMessage extends org.neo4j.bolt.v3.messaging.request.RunMessage
{
    private String databaseName;

    public RunMessage( String statement ) throws BoltIOException
    {
        this( statement, VirtualValues.EMPTY_MAP, VirtualValues.EMPTY_MAP );
    }

    public RunMessage( String statement, MapValue params ) throws BoltIOException
    {
        this( statement, params, VirtualValues.EMPTY_MAP );
    }

    public RunMessage( String statement, MapValue params, MapValue meta ) throws BoltIOException
    {
        super( statement, params, meta );
        databaseName = parseDatabaseName( meta );
    }

    @Override
    protected Bookmark parseBookmark( MapValue meta ) throws BoltIOException
    {
        return BookmarkWithDatabaseId.fromParamsOrNull( meta );
    }

    public String databaseName()
    {
        return databaseName;
    }
}
