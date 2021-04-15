/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Map;

public final class MutableDbmsOperator extends DbmsOperator
{
    private volatile Map<String,EnterpriseDatabaseState> mutableDesired;

    public MutableDbmsOperator()
    {
        super();
    }

    public MutableDbmsOperator( Map<String,EnterpriseDatabaseState> desiredStates )
    {
        super();
        mutableDesired = Map.copyOf( desiredStates );
    }

    public synchronized void setDesired( Map<String,EnterpriseDatabaseState> desiredStates )
    {
        mutableDesired = Map.copyOf( desiredStates );
    }

    @Override
    protected synchronized Map<String,EnterpriseDatabaseState> desired0()
    {
        return mutableDesired;
    }
}
