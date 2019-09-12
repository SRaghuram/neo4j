/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.fabric.planner.api;

import com.neo4j.fabric.config.FabricConfig;

import java.util.List;

import org.neo4j.cypher.internal.v4_0.ast.ReturnItem;
import org.neo4j.values.virtual.MapValue;

public interface Plan
{

    Task task();

    interface Task
    {
    }

    interface QueryTask extends Task
    {

        enum QueryMode
        {
            CAN_READ_ONLY,
            CAN_READ_WRITE
        }

        FabricConfig.Graph location();

        String query();

        MapValue parameters();

        QueryMode mode();
    }

    interface UnionTask extends Task
    {
        Task first();

        QueryTask last();

        UnionMode mode();

        enum UnionMode
        {
            ALL,
            DISTINCT
        }
    }

    interface LocalCall extends Task
    {
        enum Procedure
        {
            GET_ROUTING_TABLE
        }

        Procedure procedure();
    }

    interface LocalReturn extends Task
    {
        List<ReturnItem> items();
    }
}
