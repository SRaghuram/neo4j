/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.harness.extensionpackage;

import org.eclipse.jetty.http.HttpStatus;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;

@Path( "myExtension" )
public class MyEnterpriseUnmanagedExtension
{
    private final GraphDatabaseService db;

    public MyEnterpriseUnmanagedExtension( @Context GraphDatabaseService db )
    {
        this.db = db;
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
        try ( Transaction tx = db.beginTx() )
        {
            try ( Result result = db.execute( "CREATE CONSTRAINT ON (user:User) ASSERT exists(user.name)" ) )
            {
                // nothing to-do
            }
            tx.success();
            return Response.status( HttpStatus.CREATED_201 ).build();
        }
        catch ( Exception e )
        {
            return Response.status( HttpStatus.NOT_IMPLEMENTED_501 ).build();
        }
    }
}
