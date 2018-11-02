/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.management;

import java.util.List;

import org.neo4j.jmx.Description;
import org.neo4j.jmx.ManagementInterface;

@ManagementInterface( name = Diagnostics.NAME )
@Description( "Diagnostics provided by Neo4j" )
public interface Diagnostics
{
    String NAME = "Diagnostics";

    @Description( "Dump diagnostics information to the log." )
    void dumpToLog();

    @Description( "Dump diagnostics information for the diagnostics provider with the specified id." )
    void dumpToLog( String providerId );

    @Description( "Dump diagnostics information to JMX" )
    String dumpAll(  );

    @Description( "Extract diagnostics information for the diagnostics provider with the specified id." )
    String extract( String providerId );

    @Description( "A list of the ids for the registered diagnostics providers." )
    List<String> getDiagnosticsProviders();
}
