/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 * This file is a commercial add-on to Neo4j Enterprise Edition.
 */
package org.neo4j.kernel.ha.cluster.modeswitch;

import java.util.ArrayList;
import java.util.List;

/**
 * Container of {@link ComponentSwitcher}s that switches all contained components to master, slave or pending mode.
 * Assumed to be populated only during startup and is not thread safe.
 */
public class ComponentSwitcherContainer implements ComponentSwitcher
{
    // Modified during database startup while holding lock on the top level lifecycle instance
    private final List<ComponentSwitcher> componentSwitchers = new ArrayList<>();

    public void add( ComponentSwitcher componentSwitcher )
    {
        componentSwitchers.add( componentSwitcher );
    }

    @Override
    public void switchToMaster()
    {
        componentSwitchers.forEach( ComponentSwitcher::switchToMaster );
    }

    @Override
    public void switchToSlave()
    {
        componentSwitchers.forEach( ComponentSwitcher::switchToSlave );
    }

    @Override
    public void switchToPending()
    {
        componentSwitchers.forEach( ComponentSwitcher::switchToPending );
    }

    @Override
    public String toString()
    {
        return "ComponentSwitcherContainer{componentSwitchers=" + componentSwitchers + "}";
    }
}
