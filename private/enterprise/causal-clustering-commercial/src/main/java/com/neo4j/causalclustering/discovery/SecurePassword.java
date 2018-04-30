/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.security.SecureRandom;
import java.util.Arrays;

public class SecurePassword implements AutoCloseable
{
    private final char[] password;
    public SecurePassword( int length, SecureRandom random )
    {
        password = new char[length];
        for ( int i = 0; i < password.length; i++ )
        {
            password[i] = (char) random.nextInt( Character.MAX_VALUE + 1 );
        }
    }

    @Override
    public void close()
    {
        Arrays.fill( password, (char) 0 );
    }

    public char[] password()
    {
        return password;
    }
}
