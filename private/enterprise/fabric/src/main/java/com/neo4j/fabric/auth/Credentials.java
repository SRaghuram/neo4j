/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.auth;

public class Credentials
{
    private final String username;
    private final byte[] password;
    private final boolean provided;

    public Credentials( String username, byte[] password, boolean provided )
    {
        this.username = username;
        this.password = password;
        this.provided = provided;
    }

    public String getUsername()
    {
        return username;
    }

    public byte[] getPassword()
    {
        return password;
    }

    public boolean getProvided()
    {
        return provided;
    }
}
