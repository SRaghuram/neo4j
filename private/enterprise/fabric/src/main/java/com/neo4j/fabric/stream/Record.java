/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.stream;

import org.neo4j.values.AnyValue;

public abstract class Record
{
    public abstract AnyValue getValue( int offset );

    public abstract int size();

    @Override
    public final int hashCode()
    {
        int hashCode = 1;

        for ( var i = 0; i < size(); i++ )
        {
            hashCode = 31 * hashCode + getValue( i ).hashCode();
        }

        return hashCode;
    }

    @Override
    public final boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }

        if ( !(o instanceof Record) )
        {
            return false;
        }

        Record that = (Record) o;

        if ( this.size() != that.size() )
        {
            return false;
        }

        for ( var i = 0; i < size(); i++ )
        {
            if ( !this.getValue( i ).equals( that.getValue( i ) ) )
            {
                return false;
            }
        }

        return true;
    }
}
