/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package com.neo4j.causalclustering.core.state.storage;

import org.neo4j.io.marshal.SafeChannelMarshal;

/**
 * Wrapper class to handle ReadPastEndExceptions in a safe manner transforming it
 * to the checked EndOfStreamException which does not inherit from IOException.
 *
 * @param <STATE> The type of state marshalled.
 */
public abstract class SafeStateMarshal<STATE> extends SafeChannelMarshal<STATE> implements StateMarshal<STATE>
{
}
