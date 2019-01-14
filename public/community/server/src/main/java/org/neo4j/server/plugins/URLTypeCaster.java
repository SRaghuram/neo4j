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
package org.neo4j.server.plugins;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;

import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.repr.BadInputException;

class URLTypeCaster extends TypeCaster
{
    @Override
    Object get( GraphDatabaseAPI graphDb, ParameterList parameters, String name ) throws BadInputException
    {
        try
        {
            return parameters.getUri( name )
                    .toURL();
        }
        catch ( MalformedURLException e )
        {
            throw new BadInputException( e );
        }
    }

    @Override
    Object[] getList( GraphDatabaseAPI graphDb, ParameterList parameters, String name ) throws BadInputException
    {
        URI[] uris = parameters.getUriList( name );
        URL[] urls = new URL[uris.length];
        try
        {
            for ( int i = 0; i < urls.length; i++ )
            {
                urls[i] = uris[i].toURL();
            }
        }
        catch ( MalformedURLException e )
        {
            throw new BadInputException( e );
        }
        return urls;
    }
}
