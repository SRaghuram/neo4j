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

import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.ABSENT_DB_NAME;
import static org.neo4j.bolt.v4.messaging.MessageMetadataParser.parseDatabaseName;

public class BeginMessage extends org.neo4j.bolt.v3.messaging.request.BeginMessage
{
    private final String databaseName;

    public BeginMessage() throws BoltIOException
    {
        super();
        this.databaseName = ABSENT_DB_NAME;
    }

    public BeginMessage( MapValue meta ) throws BoltIOException
    {
        super( meta );
        this.databaseName = parseDatabaseName( meta );
    }

    @Override
    protected Bookmark parseBookmark( MapValue meta ) throws BoltIOException
    {
        return BookmarkWithDatabaseId.fromParamsOrNull( meta );
    }

    public String databaseName()
    {
        return this.databaseName;
    }
}
