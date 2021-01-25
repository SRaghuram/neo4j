/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.cc_robustness.workload;

import com.neo4j.cc_robustness.CcInstance;
import com.neo4j.cc_robustness.Orchestrator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ScriptedInstanceSelector implements InstanceSelector
{
    private final Iterator<InstanceSelector> script;
    private final String source;
    private InstanceSelector last;

    public ScriptedInstanceSelector( String script )
    {
        this.source = script;
        this.script = parseScript( script ).iterator();
    }

    private Iterable<InstanceSelector> parseScript( String script )
    {
        List<InstanceSelector> list = new ArrayList<>();
        for ( String part : script.split( "," ) )
        {
            int times = 1;
            if ( part.contains( "*" ) )
            {
                int index = part.indexOf( "*" );
                times = Integer.parseInt( part.substring( index + 1 ) );
                part = part.substring( 0, index );
            }

            InstanceSelector selector = InstanceSelectors.selector( part );
            for ( int i = 0; i < times; i++ )
            {
                list.add( selector );
            }
        }
        return list;
    }

    @Override
    public CcInstance select( Orchestrator orchestrator )
    {
        if ( script.hasNext() )
        {
            last = script.next();
        }
        return last.select( orchestrator );
    }

    @Override
    public String name()
    {
        return "ScriptedInstanceSelector[" + source + "]";
    }
}
