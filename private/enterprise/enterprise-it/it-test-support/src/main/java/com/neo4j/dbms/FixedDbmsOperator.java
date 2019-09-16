/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Map;

public class FixedDbmsOperator extends DbmsOperator
{
    private final Map<String,DatabaseState> desiredStates;

    public FixedDbmsOperator( Map<String,DatabaseState> desiredStates )
    {
        this.desiredStates = desiredStates;
    }

    @Override
    protected Map<String,DatabaseState> desired0()
    {
        return desiredStates;
    }
}
