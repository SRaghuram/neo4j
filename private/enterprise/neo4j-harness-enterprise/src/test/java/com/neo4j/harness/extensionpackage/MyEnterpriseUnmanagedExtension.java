/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.harness.extensionpackage;

import org.eclipse.jetty.http.HttpStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

@Path( "myExtension" )
public class MyEnterpriseUnmanagedExtension
{
    private final DatabaseManagementService dbms;

    public MyEnterpriseUnmanagedExtension( @Context DatabaseManagementService dbms )
    {
        this.dbms = dbms;
    }

    @GET
    @Path( "doSomething" )
    public Response doSomething()
    {
        return Response.status( 234 ).build();
    }

    @GET
    @Path( "createConstraint" )
    public Response createProperty()
    {
        GraphDatabaseService db = dbms.database( "neo4j" );
        try ( Transaction tx = db.beginTx() )
        {
            try ( Result result = tx.execute( "CREATE CONSTRAINT ON (user:User) ASSERT (user.name) IS NOT NULL" ) )
            {
                // nothing to-do
            }
            tx.commit();
            return Response.status( HttpStatus.CREATED_201 ).build();
        }
        catch ( Exception e )
        {
            return Response.status( HttpStatus.NOT_IMPLEMENTED_501 ).build();
        }
    }
}
