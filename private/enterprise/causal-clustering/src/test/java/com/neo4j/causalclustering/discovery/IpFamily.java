/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

public enum IpFamily
{
    // this assumes that localhost resolves to IPv4, can be changed if problematic (behaviour was already in place)
    IPV4( "localhost", "127.0.0.1", "0.0.0.0" ),
    IPV6( "::1", "::1", "::" );

    private final String localhostName;
    private final String localhostAddress;
    private final String wildcardAddress;

    IpFamily( String localhostName, String localhostAddress, String wildcardAddress )
    {
        this.localhostName = localhostName;
        this.localhostAddress = localhostAddress;
        this.wildcardAddress = wildcardAddress;
    }

    public String localhostName()
    {
        return localhostName;
    }

    public String localhostAddress()
    {
        return localhostAddress;
    }

    public String wildcardAddress()
    {
        return wildcardAddress;
    }
}
