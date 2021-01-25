/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.discovery;

import java.security.SecureRandom;
import java.util.Arrays;

public class SecurePassword implements AutoCloseable
{
    private final char[] password;
    private static final int lowerBound = ' ';
    private static final int upperBound = '~';
    private static final int range = upperBound - lowerBound;

    public SecurePassword( int length, SecureRandom random )
    {
        password = new char[length];
        for ( int i = 0; i < password.length; i++ )
        {
            password[i] = (char) (random.nextInt( range ) + lowerBound); // Some keystores (PKCS12 on Oracle JDK 10) check for printable ASCII range
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
