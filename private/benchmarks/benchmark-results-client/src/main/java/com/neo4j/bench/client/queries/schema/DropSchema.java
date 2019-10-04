/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is part of Neo4j internal tooling.
 */
package com.neo4j.bench.client.queries.schema;

import com.neo4j.bench.client.queries.Query;
import com.neo4j.bench.common.util.Resources;

import java.util.Optional;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;

public class DropSchema implements Query<Void>
{
    private static final String DROP_SCHEMA = Resources.fileToString( "/queries/schema/drop.cypher" );

    @Override
    public Void execute( Driver driver )
    {
        for ( String cypher : DROP_SCHEMA.split( "\n" ) )
        {
            if ( !cypher.isEmpty() && !cypher.contains( "//" ) )
            {
                System.out.println( cypher );
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

    @Override
    public Optional<String> nonFatalError()
    {
        return Optional.empty();
    }
}
