/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth.plugin.api;

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

    public AuthenticationException( String message, Throwable cause )
    {
        super( message, cause );
    }
}
