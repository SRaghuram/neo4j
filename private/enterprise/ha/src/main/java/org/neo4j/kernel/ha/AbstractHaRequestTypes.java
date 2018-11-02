/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha;

import org.neo4j.com.ObjectSerializer;
import org.neo4j.com.RequestType;
import org.neo4j.com.TargetCaller;

abstract class AbstractHaRequestTypes implements HaRequestTypes
{
    private final HaRequestType[] types = new HaRequestType[HaRequestTypes.Type.values().length];

    protected <A,B,C> void register( Type type, TargetCaller<A,B> targetCaller, ObjectSerializer<C> objectSerializer )
    {
        register( type, targetCaller, objectSerializer, true );
    }

    protected <A,B,C> void register( Type type, TargetCaller<A,B> targetCaller, ObjectSerializer<C> objectSerializer, boolean unpack )
    {
        assert types[type.ordinal()] == null;
        types[type.ordinal()] = new HaRequestType( targetCaller, objectSerializer, (byte)type.ordinal(), unpack );
    }

    @Override
    public RequestType type( Type type )
    {
        return type( (byte) type.ordinal() );
    }

    @Override
    public RequestType type( byte id )
    {
        HaRequestType requestType = types[id];
        if ( requestType == null )
        {
            throw new UnsupportedOperationException(
                    "Not used anymore, merely here to keep the ordinal ids of the others" );
        }
        return requestType;
    }
}
