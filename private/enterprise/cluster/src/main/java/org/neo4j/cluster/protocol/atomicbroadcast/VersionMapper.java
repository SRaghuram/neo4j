/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster.protocol.atomicbroadcast;

import java.util.HashMap;
import java.util.Map;

public class VersionMapper
{
    private static final Map<String, Long> classNameToSerialVersionUID = new HashMap<>();

    public long mappingFor( String className )
    {
        return classNameToSerialVersionUID.get( className );
    }

    public boolean hasMappingFor( String className )
    {
        return classNameToSerialVersionUID.containsKey( className );
    }

    public void addMappingFor( String wireClassDescriptorName, long serialVersionUID )
    {
        classNameToSerialVersionUID.put(wireClassDescriptorName, serialVersionUID);
    }
}
