/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.parser

import org.apache.spark.SparkFunSuite

import org.apache.spark.sql.catalyst.plans.SQLHelper


//noinspection ScalaStyle
class BatchParserSuite extends SparkFunSuite with SQLHelper {
  import CatalystSqlParser._

  def getText(statement: SparkStatement, batch: String): String = {
    batch.substring(statement.sourceStart, statement.sourceEnd)
  }


  test("single select") {
    val batch = "SELECT 1;"
    val tree = parseBatch(batch)
    assert(tree.statements.length == 1)
    assert(tree.statements.head.isInstanceOf[SparkStatement])
    val sparkStatement = tree.statements.head.asInstanceOf[SparkStatement]
    assert(getText(sparkStatement, batch) == "SELECT 1")
  }

  test("multi select") {
    val batch = "SELECT 1;SELECT 2;"
    val tree = parseBatch(batch)
    assert(tree.statements.length == 2)
    assert(tree.statements.forall(_.isInstanceOf[SparkStatement]))

    batch.split(";")
      .map(_.replace("\n", ""))
      .zip(tree.statements)
      .foreach { case (expected, statement) =>
        val sparkStatement = statement.asInstanceOf[SparkStatement]
        val statementText = getText(sparkStatement, batch)
        assert(statementText == expected)
      }
  }

  test("multi statement") {
    val batch = """SELECT 1;
      |SELECT 2;
      |INSERT INTO A VALUES (a, b, 3);
      |SELECT a, b, c FROM T;
      |SELECT * FROM T;
        """.stripMargin
    val tree = parseBatch(batch)
    assert(tree.statements.length == 5)
    assert(tree.statements.forall(_.isInstanceOf[SparkStatement]))
    batch.split(";")
      .map(_.replace("\n", ""))
      .zip(tree.statements)
      .foreach { case (expected, statement) =>
      val sparkStatement = statement.asInstanceOf[SparkStatement]
      val statementText = getText(sparkStatement, batch)
      assert(statementText == expected)
    }
  }

  test("if") {
    val batch =
      """IF 1 = 1 THEN
        |  SELECT 1;
        |ELSE
        |  SELECT 2;
        |END IF;
        """.stripMargin
    val tree = parseBatch(batch)
    assert(tree.statements.length == 1)
    assert(tree.statements.head.isInstanceOf[CiglaIfElseStatement])
    val ifStmt = tree.statements.head.asInstanceOf[CiglaIfElseStatement]
    assert(ifStmt.ifBody.statements.length == 1)
    assert(ifStmt.ifBody.statements.head.isInstanceOf[SparkStatement])
    assert(getText(ifStmt.ifBody.statements.head.asInstanceOf[SparkStatement], batch) == "SELECT 1")

    assert(ifStmt.elseBody.isDefined)
    assert(ifStmt.elseBody.get.statements.length == 1)
    assert(ifStmt.elseBody.get.statements.head.isInstanceOf[SparkStatement])
    assert(getText(ifStmt.elseBody.get.statements.head.asInstanceOf[SparkStatement], batch) == "SELECT 2")
  }
}
