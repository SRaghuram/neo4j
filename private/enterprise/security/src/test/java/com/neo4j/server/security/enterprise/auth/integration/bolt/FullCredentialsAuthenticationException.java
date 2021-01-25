/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth.integration.bolt;

import org.neo4j.driver.exceptions.AuthenticationException;

public class FullCredentialsAuthenticationException extends AuthenticationException
{
    private AuthenticationException authExc;
    private String username, password;

    FullCredentialsAuthenticationException( AuthenticationException e, String username, String password )
    {
        super( e.code(), e.getMessage() );
        authExc = e;
        this.username = username;
        this.password = password;
    }

    @Override
    public String getMessage()
    {
        return authExc.getMessage() + " The provided username and password was '" + username + "' and '" + password + "'.";
    }

    @Override
    public void printStackTrace()
    {
        authExc.printStackTrace();
    }
}
