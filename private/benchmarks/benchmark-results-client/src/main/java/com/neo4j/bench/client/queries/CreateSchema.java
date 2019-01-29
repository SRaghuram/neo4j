package com.neo4j.bench.client.queries;

import com.neo4j.bench.client.util.Resources;

import java.util.Optional;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;

public class CreateSchema implements Query<Void>
{
    private static final String CREATE_SCHEMA = Resources.fileToString( "/queries/schema/create.cypher" );

    @Override
    public Void execute( Driver driver )
    {
        for ( String cypher : CREATE_SCHEMA.split( "\n" ) )
        {
            if ( !cypher.isEmpty() && !cypher.contains( "//" ) )
            {
                System.out.println( cypher );
                try ( Session session = driver.session() )
                {
                    session.run( cypher );
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
