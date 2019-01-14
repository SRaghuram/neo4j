/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.cluster;

import java.net.URI;

/**
* Listener for binding to a server address.
*/
public interface BindingListener
{
    void listeningAt( URI me );
}
