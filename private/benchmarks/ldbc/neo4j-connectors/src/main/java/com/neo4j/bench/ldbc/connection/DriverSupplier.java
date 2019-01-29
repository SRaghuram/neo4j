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

package com.neo4j.bench.ldbc.connection;

import java.io.Closeable;
import java.net.URI;
import java.util.function.Supplier;

import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Logging;

public class DriverSupplier implements Supplier<Driver>, Closeable
{
    private final Logging logging;
    private final URI uri;
    private final AuthToken authToken;

    private Driver driver;

    public DriverSupplier( Logging logging, URI uri, AuthToken authToken )
    {
        this.logging = logging;
        this.uri = uri;
        this.authToken = authToken;
    }

    @Override
    public Driver get()
    {
        if ( driver == null )
        {
            this.driver = GraphDatabase.driver( uri, authToken, Config.build().withLogging( logging )
                                                                      .withEncryptionLevel( Config.EncryptionLevel.NONE )
                                                                      .toConfig() );
        }
        return driver;
    }

    @Override
    public void close()
    {
        if ( driver != null )
        {
            driver.close();
        }
    }
}
