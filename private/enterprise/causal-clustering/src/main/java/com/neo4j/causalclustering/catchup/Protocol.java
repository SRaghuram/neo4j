/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.catchup;

import java.util.Map;

public abstract class Protocol<E extends Enum<E>>
{
    private E state;

    protected Protocol( E initialValue )
    {
        this.state = initialValue;
    }

    public void expect( E state )
    {
        this.state = state;
    }

    public boolean isExpecting( E state )
    {
        return this.state == state;
    }

    public <T> T select( Map<E,T> map )
    {
        return map.get( state );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "{" + "state=" + state + '}';
    }
}
