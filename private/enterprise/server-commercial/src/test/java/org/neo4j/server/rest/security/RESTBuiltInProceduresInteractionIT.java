/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.server.rest.security;

import org.junit.Rule;

import java.util.Map;

import org.neo4j.server.security.enterprise.auth.BuiltInProceduresInteractionTestBase;
import org.neo4j.server.security.enterprise.auth.NeoInteractionLevel;
import org.neo4j.test.rule.SuppressOutput;

import static org.neo4j.test.rule.SuppressOutput.suppressAll;

public class RESTBuiltInProceduresInteractionIT extends BuiltInProceduresInteractionTestBase<RESTSubject>
{
    @Rule
    public SuppressOutput suppressOutput = suppressAll();

    public RESTBuiltInProceduresInteractionIT()
    {
        super();
        CHANGE_PWD_ERR_MSG = "User is required to change their password.";
        PWD_CHANGE_CHECK_FIRST = true;
        IS_EMBEDDED = false;
    }

    @Override
    public NeoInteractionLevel<RESTSubject> setUpNeoServer( Map<String, String> config ) throws Throwable
    {
        return new RESTInteraction( config );
    }

    @Override
    protected Object valueOf( Object obj )
    {
        if ( obj instanceof Long )
        {
            return ((Long) obj).intValue();
        }
        else
        {
            return obj;
        }
    }
}
