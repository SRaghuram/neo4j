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
package org.neo4j.kernel.impl.transaction.log;

import java.io.IOException;

import org.neo4j.kernel.impl.index.IndexCommand;
import org.neo4j.storageengine.api.StorageCommand;

final class IndexCommandDetector implements org.neo4j.helpers.collection.Visitor<StorageCommand,IOException>
{
    private boolean hasWrittenAnyExplicitIndexCommand;

    public void reset()
    {
        hasWrittenAnyExplicitIndexCommand = false;
    }

    @Override
    public boolean visit( StorageCommand element ) throws IOException
    {
        if ( element instanceof IndexCommand )
        {
            hasWrittenAnyExplicitIndexCommand = true;
        }
        return false;
    }

    public boolean hasWrittenAnyExplicitIndexCommand()
    {
        return hasWrittenAnyExplicitIndexCommand;
    }
}
