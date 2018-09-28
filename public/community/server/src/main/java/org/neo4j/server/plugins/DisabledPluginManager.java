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
package org.neo4j.server.plugins;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.repr.ExtensionPointRepresentation;
import org.neo4j.server.rest.repr.Representation;

/**
 * @deprecated Server plugins are deprecated for removal in the next major release. Please use unmanaged extensions instead.
 */
@Deprecated
public class DisabledPluginManager implements PluginManager
{
    public static final PluginManager INSTANCE = new DisabledPluginManager();

    private DisabledPluginManager()
    {
    }

    @Deprecated
    @Override
    public <T> Representation invoke( GraphDatabaseAPI graphDb, String name, Class<T> type, String method, T context, ParameterList params )
    {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    @Override
    public ExtensionPointRepresentation describe( String name, Class<?> type, String method )
    {
        throw new UnsupportedOperationException();
    }

    @Deprecated
    @Override
    public List<ExtensionPointRepresentation> describeAll( String extensionName )
    {
        return Collections.emptyList();
    }

    @Deprecated
    @Override
    public Set<String> extensionNames()
    {
        return Collections.emptySet();
    }

    @Deprecated
    @Override
    public Map<String,List<String>> getExensionsFor( Class<?> type )
    {
        return Collections.emptyMap();
    }
}
