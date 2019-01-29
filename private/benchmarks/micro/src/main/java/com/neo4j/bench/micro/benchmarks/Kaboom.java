package com.neo4j.bench.micro.benchmarks;

public class Kaboom extends RuntimeException
{
    public Kaboom()
    {
        super( "This was entirely unexpected" );
    }

    public Kaboom( String message )
    {
        super( message );
    }

    public Kaboom( String message, Throwable cause )
    {
        super( message, cause );
    }
}
