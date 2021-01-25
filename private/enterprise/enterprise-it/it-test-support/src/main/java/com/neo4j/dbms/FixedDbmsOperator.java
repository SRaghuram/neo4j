/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Map;

public class FixedDbmsOperator extends DbmsOperator
{
    private final Map<String,EnterpriseDatabaseState> desiredStates;

    public FixedDbmsOperator( Map<String,EnterpriseDatabaseState> desiredStates )
    {
        this.desiredStates = desiredStates;
    }

    @Override
    protected Map<String,EnterpriseDatabaseState> desired0()
    {
        return desiredStates;
    }
}
