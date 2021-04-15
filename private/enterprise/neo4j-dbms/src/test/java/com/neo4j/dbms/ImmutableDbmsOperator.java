/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class ImmutableDbmsOperator extends DbmsOperator
{
    private final Map<String,EnterpriseDatabaseState> desired;

    public ImmutableDbmsOperator( Map<String,EnterpriseDatabaseState> desiredStates )
    {
        this.desired = new ConcurrentHashMap<>( desiredStates );
    }

    @Override
    protected Map<String,EnterpriseDatabaseState> desired0()
    {
        return desired;
    }
}
