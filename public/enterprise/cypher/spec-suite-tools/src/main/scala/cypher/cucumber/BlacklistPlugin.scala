/*
 * Copyright (c) 2002-2019 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.cucumber

import java.io.FileNotFoundException
import java.net.URI
import java.nio.charset.StandardCharsets
import java.util

import gherkin.formatter.model.{Match, Result}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source

object BlacklistPlugin {
  private var _uri:URI = null
  private var _blacklist: Set[String] = null
  private val _usedScenarios: mutable.Set[String] = mutable.Set()

  def blacklisted(name: String) = {
    val newName = normalizedScenarioName(name)
    _usedScenarios.add(newName)
    blacklist().contains(newName)
  }

  def getDiffBetweenBlacklistAndUsedScenarios(): util.Set[String] = {
    blacklist().diff(_usedScenarios).asJava
  }

  def normalizedScenarioName(name: String) = {
    val builder = new StringBuilder

    var inBlanks = false
    name.trim.foreach { (ch) =>
      if (ch == ' ') {
        if (!inBlanks) {
          builder += ' '
          inBlanks = true
        }
      } else {
        builder += ch.toLower
        inBlanks = false
      }
    }

    val result = builder.toString()
    result
  }

  private def blacklist(): Set[String] = {
    assert(_blacklist != null)
    _blacklist
  }
}

class BlacklistPlugin(blacklistFile: URI) extends CucumberAdapter {

  override def before(`match`: Match, result: Result): Unit = {
    if (!blacklistFile.equals(BlacklistPlugin._uri) ) {
      // only do this the first time for each new URI
      BlacklistPlugin._usedScenarios.clear()
      BlacklistPlugin._uri = blacklistFile
      val url = getClass.getResource(blacklistFile.getPath)
      if (url == null) throw new FileNotFoundException(s"blacklist file not found at: $blacklistFile")
      val itr = Source.fromFile(url.getPath, StandardCharsets.UTF_8.name()).getLines()
      BlacklistPlugin._blacklist = itr.foldLeft(Set.empty[String]) {
        case (set, scenarioName) =>
          val normalizedName = BlacklistPlugin.normalizedScenarioName(scenarioName)
          if (normalizedName.isEmpty || normalizedName.startsWith("//")) set else set + normalizedName
      }
    }
  }
}
