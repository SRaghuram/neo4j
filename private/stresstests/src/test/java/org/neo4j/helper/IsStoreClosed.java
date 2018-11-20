/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.helper;

import java.util.function.Predicate;

import org.neo4j.exceptions.UnsatisfiedDependencyException;

public class IsStoreClosed implements Predicate<Throwable>
{
    @Override
    public boolean test( Throwable ex )
    {

        if ( ex == null )
        {
            return false;
        }

        if ( ex instanceof UnsatisfiedDependencyException )
        {
            return true;
        }

        if ( ex instanceof IllegalStateException )
        {
            String message = ex.getMessage();
            return message.startsWith( "MetaDataStore for file " ) && message.endsWith( " is closed" );
        }

        return test( ex.getCause() );
    }
}
