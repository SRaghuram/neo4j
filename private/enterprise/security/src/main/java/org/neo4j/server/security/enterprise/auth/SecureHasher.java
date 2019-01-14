/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.security.enterprise.auth;

import org.apache.shiro.authc.credential.HashedCredentialsMatcher;
import org.apache.shiro.crypto.RandomNumberGenerator;
import org.apache.shiro.crypto.SecureRandomNumberGenerator;
import org.apache.shiro.crypto.hash.SimpleHash;
import org.apache.shiro.util.ByteSource;

import java.util.HashMap;
import java.util.Map;

public class SecureHasher
{
    // TODO: Do we need to make this configurable?
    private static final String HASH_ALGORITHM = "SHA-256";
    private static final int HASH_ITERATIONS = 1024;
    private static final int SALT_BYTES_SIZE = 32;

    private RandomNumberGenerator randomNumberGenerator;
    private HashedCredentialsMatcher hashedCredentialsMatcher;
    private Map<Integer,HashedCredentialsMatcher> hashedCredentialsMatchers;

    private RandomNumberGenerator getRandomNumberGenerator()
    {
        if ( randomNumberGenerator == null )
        {
            randomNumberGenerator = new SecureRandomNumberGenerator();
        }

        return randomNumberGenerator;
    }

    public SimpleHash hash( byte[] source )
    {
        ByteSource salt = generateRandomSalt( SALT_BYTES_SIZE );
        return new SimpleHash( HASH_ALGORITHM, source, salt, HASH_ITERATIONS );
    }

    public HashedCredentialsMatcher getHashedCredentialsMatcher()
    {
        if ( hashedCredentialsMatcher == null )
        {
            hashedCredentialsMatcher = new HashedCredentialsMatcher( HASH_ALGORITHM );
            hashedCredentialsMatcher.setHashIterations( HASH_ITERATIONS );
        }

        return hashedCredentialsMatcher;
    }

    public HashedCredentialsMatcher getHashedCredentialsMatcherWithIterations( int iterations )
    {
        if ( hashedCredentialsMatchers == null )
        {
            hashedCredentialsMatchers = new HashMap<>();
        }

        HashedCredentialsMatcher matcher = hashedCredentialsMatchers.get( iterations );
        if ( matcher == null )
        {
            matcher = new HashedCredentialsMatcher( HASH_ALGORITHM );
            matcher.setHashIterations( iterations );
            hashedCredentialsMatchers.put( iterations, matcher );
        }

        return matcher;
    }

    private ByteSource generateRandomSalt( int bytesSize )
    {
        return getRandomNumberGenerator().nextBytes( bytesSize );
    }
}
