/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 *
 */

package com.neo4j.bench.ldbc.utils;

import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class UtilsTests
{
    @Test
    public void shouldCopyAndAppendElementToNewArrayWhenOldArrayNotNull() throws IOException
    {
        String[] oldArray = {"1", "2", "3"};
        String newElement = "4";
        String[] newArray = Utils.copyArrayAndAddElement( oldArray, newElement );
        assertThat( newArray, equalTo( new String[] {"1", "2", "3", "4"} ) );
    }

    @Test
    public void shouldCopyAndAppendElementToNewArrayWhenOldArrayNull() throws IOException
    {
        String[] oldArray = null;
        String newElement = "4";
        String[] newArray = Utils.copyArrayAndAddElement( oldArray, newElement );
        assertThat( newArray, equalTo( new String[] {"4"} ) );
    }
}
