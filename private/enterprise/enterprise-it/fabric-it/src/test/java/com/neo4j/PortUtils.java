/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j;

import java.io.IOException;
import java.net.ServerSocket;

public class PortUtils
{

    public static class Ports
    {
        public final int bolt;
        public final int http;

        public Ports( int bolt, int http )
        {
            this.bolt = bolt;
            this.http = http;
        }
    }

    public static Ports findFreePorts()
    {
        try ( ServerSocket s1 = new ServerSocket( 0 );
                ServerSocket s2 = new ServerSocket( 0 ) )
        {
            return new Ports( s1.getLocalPort(), s2.getLocalPort() );
        }
        catch ( IOException e )
        {
            e.printStackTrace();
            return new Ports( 7687, 7474 );
        }
    }
}
