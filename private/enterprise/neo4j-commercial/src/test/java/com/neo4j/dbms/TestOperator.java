/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Map;

import org.neo4j.kernel.database.DatabaseId;

import static java.util.Collections.unmodifiableMap;

public class TestOperator implements Operator
{
    private final Map<DatabaseId,OperatorState> desired;

    TestOperator( Map<DatabaseId,OperatorState> desired )
    {
        this.desired = desired;
    }

    @Override
    public Map<DatabaseId,OperatorState> getDesired()
    {
        return unmodifiableMap( desired );
    }
}
