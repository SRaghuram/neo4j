/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.plugin.api;

/**
 * An exception that can be thrown on authentication.
 * Throwing this exception will cause authentication to fail.
 */
public class AuthenticationException extends Exception
{
    public AuthenticationException( String message )
    {
        super( message );
    }
}
