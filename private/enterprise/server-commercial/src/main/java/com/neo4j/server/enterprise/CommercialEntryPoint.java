/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.enterprise;

import org.neo4j.server.ServerBootstrapper;

public class CommercialEntryPoint
{
    private CommercialEntryPoint()
    {
    }

    public static void main( String[] args )
    {
        int status = ServerBootstrapper.start( new CommercialBootstrapper(), args );
        if ( status != 0 )
        {
            System.exit( status );
        }
    }
}
