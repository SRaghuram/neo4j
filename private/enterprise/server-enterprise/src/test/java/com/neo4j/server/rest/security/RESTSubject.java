/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.security;

public class RESTSubject
{
    String principalCredentials;
    String username;
    String password;

    public RESTSubject( String username, String password, String principalCredentials )
    {
        this.username = username;
        this.password = password;
        this.principalCredentials = principalCredentials;
    }
}
