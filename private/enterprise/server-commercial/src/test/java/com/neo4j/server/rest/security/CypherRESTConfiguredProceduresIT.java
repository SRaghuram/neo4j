/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.security;

import org.junit.Rule;

import java.util.Map;

import org.neo4j.server.security.enterprise.auth.ConfiguredProceduresTestBase;
import org.neo4j.server.security.enterprise.auth.NeoInteractionLevel;
import org.neo4j.test.rule.SuppressOutput;

import static org.neo4j.test.rule.SuppressOutput.suppressAll;

public class CypherRESTConfiguredProceduresIT extends ConfiguredProceduresTestBase<com.neo4j.server.rest.security.RESTSubject>
{
    @Rule
    public SuppressOutput suppressOutput = suppressAll();

    public CypherRESTConfiguredProceduresIT()
    {
        super();
        CHANGE_PWD_ERR_MSG = "User is required to change their password.";
        PWD_CHANGE_CHECK_FIRST = true;
        IS_EMBEDDED = false;
    }

    @Override
    public NeoInteractionLevel<com.neo4j.server.rest.security.RESTSubject> setUpNeoServer(Map<String, String> config ) throws Throwable
    {
        return new com.neo4j.server.rest.security.CypherRESTInteraction( config );
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
