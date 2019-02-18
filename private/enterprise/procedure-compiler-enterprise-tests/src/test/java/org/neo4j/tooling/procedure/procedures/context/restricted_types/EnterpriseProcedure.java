/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.tooling.procedure.procedures.context.restricted_types;

import com.neo4j.kernel.enterprise.api.security.CommercialAuthManager;
import com.neo4j.server.security.enterprise.log.SecurityLog;

import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Procedure;

public class EnterpriseProcedure
{
    @Context
    public GraphDatabaseService graphDatabaseService;

    @Context
    public CommercialAuthManager commercialAuthManager;

    @Context
    public SecurityLog securityLog;

    @Procedure
    public Stream<MyRecord> procedure()
    {
        return Stream.empty();
    }

    public static class MyRecord
    {
        public String property;
    }
}
