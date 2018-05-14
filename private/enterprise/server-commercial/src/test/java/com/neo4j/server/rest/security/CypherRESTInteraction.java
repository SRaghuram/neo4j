/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.security;

import org.codehaus.jackson.JsonNode;

import java.io.IOException;
import java.util.Map;
import java.util.function.Consumer;
import javax.ws.rs.core.HttpHeaders;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.test.server.HTTP;

import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

class CypherRESTInteraction extends com.neo4j.server.rest.security.AbstractRESTInteraction
{

    CypherRESTInteraction( Map<String,String> config ) throws IOException
    {
        super( config );
    }

    @Override
    String commitPath()
    {
        return "db/data/cypher";
    }

    @Override
    HTTP.RawPayload constructQuery( String query )
    {
        return quotedJson( " { 'query': '" + query.replace( "'", "\\'" ).replace( "\"", "\\\"" ) + "' }" );
    }

    @Override
    void consume( Consumer<ResourceIterator<Map<String,Object>>> resultConsumer, JsonNode data )
    {
        if ( data.has( "data" ) && data.get( "data" ).has( 0 ) )
        {
            resultConsumer.accept( new CypherRESTResult( data ) );
        }
    }

    @Override
    protected HTTP.Response authenticate( String principalCredentials )
    {
        return HTTP.withHeaders( HttpHeaders.AUTHORIZATION, principalCredentials )
                .request( POST, commitURL(), constructQuery( "RETURN 1" ) );
    }

    private class CypherRESTResult extends AbstractRESTResult
    {
        CypherRESTResult( JsonNode fullResult )
        {
            super( fullResult );
        }

        @Override
        protected JsonNode getRow( JsonNode data, int i )
        {
            return data.get( i );
        }
    }
}
