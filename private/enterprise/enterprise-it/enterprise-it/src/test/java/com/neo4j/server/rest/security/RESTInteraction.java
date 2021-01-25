/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.security;

import com.fasterxml.jackson.databind.JsonNode;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.function.Consumer;
import javax.ws.rs.core.HttpHeaders;

import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.test.server.HTTP;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

class RESTInteraction extends AbstractRESTInteraction
{

    RESTInteraction( Map<Setting<?>,String> config, Path dataDir ) throws IOException
    {
        super( config, dataDir );
    }

    @Override
    String commitPath( String database )
    {
        return String.format( "db/%s/tx/commit", database );
    }

    @Override
    HTTP.RawPayload constructQuery( String query )
    {
        return quotedJson( "{'statements':[{'statement':'" +
                           query.replace( "'", "\\'" ).replace( "\"", "\\\"" )
                           + "'}]}" );
    }

    @Override
    void consume( Consumer<ResourceIterator<Map<String,Object>>> resultConsumer, JsonNode data )
    {
        if ( data.has( "results" ) && data.get( "results" ).has( 0 ) )
        {
            resultConsumer.accept( new RESTResult( data.get( "results" ).get( 0 ) ) );
        }
    }

    @Override
    protected HTTP.Response authenticate( String principalCredentials )
    {
        return HTTP.withHeaders( HttpHeaders.AUTHORIZATION, principalCredentials ).POST( commitURL( DEFAULT_DATABASE_NAME ) );
    }

    private class RESTResult extends AbstractRESTResult
    {
        RESTResult( JsonNode fullResult )
        {
            super( fullResult );
        }

        @Override
        protected JsonNode getRow( JsonNode data, int i )
        {
            return data.get( i ).get( "row" );
        }
    }
}
