/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.server.rest.security;

import org.junit.Rule;

import java.util.Map;

import org.neo4j.server.security.enterprise.auth.ConfiguredProceduresTestBase;
import org.neo4j.server.security.enterprise.auth.NeoInteractionLevel;
import org.neo4j.test.rule.SuppressOutput;

import static org.neo4j.test.rule.SuppressOutput.suppressAll;

public class RESTConfiguredProceduresIT extends ConfiguredProceduresTestBase<RESTSubject>
{
    @Rule
    public SuppressOutput suppressOutput = suppressAll();

    public RESTConfiguredProceduresIT()
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
