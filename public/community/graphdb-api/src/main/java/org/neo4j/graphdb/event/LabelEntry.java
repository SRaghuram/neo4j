/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.graphdb.event;

import org.neo4j.annotations.api.PublicApi;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;

/**
 * Represents an assigned or removed label for a node.
 */
@PublicApi
public interface LabelEntry
{
    /**
     * This is the label that has been added or removed.
     *
     * @return the label.
     */
    Label label();

    /**
     * This is the node which has had the label added or removed.
     *
     * @return the node that has been modified.
     */
    Node node();

}
