/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.server.rest.repr;

import java.util.function.Supplier;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

public class InputFormatProvider implements Supplier<InputFormat>
{
    @Context
    private RepresentationFormatRepository repository;

    @Context
    private ContainerRequestContext requestContext;

    @Override
    public InputFormat get()
    {
        try
        {
            return repository.inputFormat( requestContext.getMediaType() );
        }
        catch ( MediaTypeNotSupportedException e )
        {
            throw new WebApplicationException( Response.status( Status.UNSUPPORTED_MEDIA_TYPE )
                    .entity( e.getMessage() )
                    .build() );
        }
    }
}
