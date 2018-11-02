/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.com;

import java.net.InetSocketAddress;

public class ServerUtil
{

    private ServerUtil()
    {
    }

    public static String getHostString( InetSocketAddress socketAddress )
    {
        if ( socketAddress.isUnresolved() )
        {
            return socketAddress.getHostName();
        }
        else
        {
            return socketAddress.getAddress().getHostAddress();
        }
    }
}
