/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.test;

import org.neo4j.cypher.internal.javacompat.ResultSubscriber;

public class TestFabricResultSubscriber extends ResultSubscriber
{
    TestFabricResultSubscriber() {
        super(null, new TestFabricValueMapper());
    }
}
