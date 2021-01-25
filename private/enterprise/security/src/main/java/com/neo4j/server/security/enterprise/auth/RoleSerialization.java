/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.security.enterprise.auth;

import java.util.SortedSet;
import java.util.TreeSet;

import org.neo4j.cypher.internal.security.FormatException;
import org.neo4j.server.security.auth.FileRepositorySerializer;

import static java.lang.String.format;

/**
 * Serializes role authorization and authentication data to a format similar to unix passwd files.
 */
public class RoleSerialization extends FileRepositorySerializer<RoleRecord>
{
    private static final String roleSeparator = ":";
    private static final String userSeparator = ",";

    @Override
    protected String serialize( RoleRecord role )
    {
        return String.join( roleSeparator, role.name(), String.join( userSeparator, role.users() ) );
    }

    @Override
    protected RoleRecord deserializeRecord( String line, int lineNumber ) throws FormatException
    {
        String[] parts = line.split( roleSeparator, -1 );
        if ( parts.length != 2 )
        {
            throw new FormatException( format( "wrong number of line fields [line %d]", lineNumber ) );
        }
        return new RoleRecord.Builder()
                .withName( parts[0] )
                .withUsers( deserializeUsers( parts[1] ) )
                .build();
    }

    private SortedSet<String> deserializeUsers( String part )
    {
        String[] splits = part.split( userSeparator, -1 );

        SortedSet<String> users = new TreeSet<>();

        for ( String user : splits )
        {
            if ( !user.trim().isEmpty() )
            {
                users.add( user );
            }
        }

        return users;
    }
}
