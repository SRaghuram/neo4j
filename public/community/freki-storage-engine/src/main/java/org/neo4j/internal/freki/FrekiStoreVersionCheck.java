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

import java.util.Optional;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;
import org.neo4j.storageengine.api.StoreVersion;
import org.neo4j.storageengine.api.StoreVersionCheck;

class FrekiStoreVersionCheck implements StoreVersionCheck
{
    @Override
    public Optional<String> storeVersion( PageCursorTracer cursorTracer )
    {
        return Optional.empty();
    }

    @Override
    public String configuredVersion()
    {
        return FrekiStoreVersion.VERSION;
    }

    @Override
    public StoreVersion versionInformation( String storeVersion )
    {
        return new FrekiStoreVersion();
    }

    @Override
    public Result checkUpgrade( String desiredVersion, PageCursorTracer cursorTracer )
    {
        return new Result( Outcome.ok, FrekiStoreVersion.VERSION, null );
    }
}
