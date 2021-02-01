/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.schema;

import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.common.util.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;

public class CreateSchema implements Query<Void>
{

    private static final Logger LOG = LoggerFactory.getLogger( CreateSchema.class );

    private static final String CREATE_SCHEMA = Resources.fileToString( "/queries/schema/create.cypher" );

    @Override
    public Void execute( Driver driver )
    {
        for ( String cypher : CREATE_SCHEMA.split( "\n" ) )
        {
            if ( !cypher.isEmpty() && !cypher.contains( "//" ) )
            {
                LOG.debug( cypher );
                try ( Session session = driver.session() )
                {
                    session.run( cypher );
                }
                catch ( Exception e )
                {
                    System.out.println( "Error executing statement: " + cypher + ": " + e.getMessage() );
                }
            }
        }
        return null;
    }
}
