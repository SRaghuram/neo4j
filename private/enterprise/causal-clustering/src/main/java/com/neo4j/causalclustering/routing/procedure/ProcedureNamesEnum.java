/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.routing.procedure;

import java.util.Arrays;

import static java.lang.String.join;

public interface ProcedureNamesEnum
{
     String[] procedureNameSpace();
     String procedureName();

     default String[] fullyQualifiedProcedureName()
     {
         String[] namespace = procedureNameSpace();
         String[] fullyQualifiedProcedureName = Arrays.copyOf( namespace, namespace.length + 1 );
         fullyQualifiedProcedureName[namespace.length] = procedureName();
         return fullyQualifiedProcedureName;
     }

     default String callName()
     {
         return join( ".", procedureNameSpace()) + "." + procedureName();
     }

}
