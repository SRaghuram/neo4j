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
package org.neo4j.ext.udc.impl;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class PingerHandler extends AbstractHandler
{
    private final Map<String,String> queryMap = new ConcurrentHashMap<>();

    @Override
    public void handle( String target, Request baseRequest, HttpServletRequest request, HttpServletResponse response )
    {
        String query = request.getQueryString();
        if ( query != null )
        {
            String[] params = query.split( "\\+" );
            if ( params.length > 0 )
            {
                for ( String param : params )
                {
                    String[] pair = param.split( "=" );
                    String key = URLDecoder.decode( pair[0], StandardCharsets.UTF_8 );
                    String value = URLDecoder.decode( pair[1], StandardCharsets.UTF_8 );
                    queryMap.put( key, value );
                }
            }
        }
        baseRequest.setHandled( true );
    }

    public Map<String, String> getQueryMap()
    {
        return queryMap;
    }
}
