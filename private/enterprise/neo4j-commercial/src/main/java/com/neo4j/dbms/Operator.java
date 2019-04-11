/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.dbms;

import java.util.Map;

import org.neo4j.kernel.database.DatabaseId;

public interface Operator
{
    Map<DatabaseId,OperatorState> getDesired();
}
