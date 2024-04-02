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
class CiglaLangParserSuite extends SparkFunSuite with SQLHelper {

  // Dummy evaluator that always returns true.
  case object AlwaysTrueEval extends StatementBooleanEvaluator {
    override def eval(statement: BoolEvaluableStatement): Boolean = true
  }

  def buildTree(batch: String): CiglaBody = {
    val cp = new CiglaParser()
    val parser = cp.parseBatch(batch)(t => t)
    val astBuilder = CiglaLangBuilder(batch, AlwaysTrueEval, CatalystSqlParser)
    astBuilder.visitBody(parser.body())
  }

  test("single select") {
    val batch = "SELECT 1;"
    val tree = buildTree(batch)
    assert(tree.statements.length == 1)
    assert(tree.statements.head.asInstanceOf[SparkStatement].command == "SELECT 1")
  }

  test("Initial parsing test") {
    val batch =
      """
        |INSERT INTO a VALUES (1, 2, x);
        |SELECT BLA;
        |SELECT 1, 2;
        |SELECT a, b, c FROM T;
        |SELECT a.b.c, d.e;
        |SELECT a, b FROM T WHERE x=y;
        |SELECT * FROM T;
        |SELECT COUNT(*) > 2 FROM T;
        |SELECT 2;
        |SELECT 3;""".stripMargin

    val tree = buildTree(batch)

    assert(tree.statements.length == batch.split(";").length)
    batch.split(";").zip(tree.statements).foreach {
      case (expected, actual: SparkStatement) => assert(expected.trim === actual.command.trim)
    }
  }

  test("Insert statement") {
    val batch =
      """
        |INSERT INTO a VALUES (1, 2, x);
        |INSERT INTO a VALUES (a, b, c);""".stripMargin

    val tree = buildTree(batch)
    assert(tree.statements.length == batch.split(";").length)
    batch.split(";").zip(tree.statements).foreach {
      case (expected, actual: SparkStatement) => assert(expected.trim === actual.command.trim)
    }
  }

  test("Multiline statement") {
    val batch =
      """
        |SELECT a, b, c
        |FROM T
        |WHERE x=y;""".stripMargin

    val tree = buildTree(batch)
    assert(tree.statements.length == batch.split(";").length)
    batch.split(";").zip(tree.statements).foreach {
      case (expected, actual: SparkStatement) => assert(expected.trim === actual.command.trim)
    }
  }

  test("select case") {
    val batch =
      """
        |SELECT CASE WHEN COUNT(*) > 10 THEN true
        |ELSE false
        |END as MoreThanTen
        |FROM t;
        |""".stripMargin

    val tree = buildTree(batch)
    assert(tree.statements.length == 1)

    batch.split(";").zip(tree.statements).foreach {
      case (expected, actual: SparkStatement) => assert(expected.trim === actual.command.trim)
    }
  }

  test("if else") {
    val batch =
      """
        |IF (SELECT 1) THEN
        |  SELECT 2;
        |ELSE
        |  SELECT 3;
        |END IF;""".stripMargin

    val tree = buildTree(batch)

    tree.statements.foreach {
      case ifElse: CiglaIfElseStatement =>
        assert(ifElse.condition.asInstanceOf[SparkStatement].command == "SELECT 1")
        assert(ifElse.ifBody.statements.head.asInstanceOf[SparkStatement].command == "SELECT 2")
        assert(ifElse.elseBody.head.statements.head.asInstanceOf[SparkStatement].command == "SELECT 3")
    }
  }

  test("if else with expression") {
    val batch = """
      |IF (1=1) THEN
      |  SELECT 2;
      |ELSE
      |  SELECT 3;
      |END IF;""".stripMargin

    val tree = buildTree(batch)

    tree.statements.foreach {
      case ifElse: CiglaIfElseStatement =>
        assert(ifElse.condition.asInstanceOf[SparkStatement].command == "1=1")
        assert(ifElse.ifBody.statements.head.asInstanceOf[SparkStatement].command == "SELECT 2")
        assert(ifElse.elseBody.head.statements.head.asInstanceOf[SparkStatement].command == "SELECT 3")
    }
  }

  test("create table") {
    val batch =
      """
        |CREATE TABLE a (a INT, b STRING, c DOUBLE) USING parquet;
        |CREATE TABLE a (a INT, b STRING, c DOUBLE) USING parquet;
        |""".stripMargin

    val tree = buildTree(batch)
    batch.split(";").zip(tree.statements).foreach {
      case (expected, actual: SparkStatement) => assert(expected.trim === actual.command.trim)
    }
  }

  test("while loop") {
    val batch =
      """
        |WHILE (SELECT 1) DO
        | SELECT 2;
        | SELECT 3;
        |END WHILE;
        |""".stripMargin

    val tree = buildTree(batch)

    tree.statements.foreach {
      case whileStmt: CiglaWhileStatement =>
        assert(whileStmt.condition.asInstanceOf[SparkStatement].command == "SELECT 1")
        assert(whileStmt.whileBody.asInstanceOf[CiglaBody].statements.head.asInstanceOf[SparkStatement].command == "SELECT 2")
        assert(whileStmt.whileBody.asInstanceOf[CiglaBody].statements(1).asInstanceOf[SparkStatement].command == "SELECT 3")
    }
  }

  test("; in string") {
    val batch =
      """
        |SELECT 'SELECT 1; SELECT 2;';
        |""".stripMargin

    val tree = buildTree(batch)
    assert(tree.statements.length == 1)
    assert(tree.statements.head.asInstanceOf[SparkStatement].command == "SELECT 'SELECT 1; SELECT 2;'")
  }

  test("parse variable") {
    val batch =
      """
        |DECLARE x = 'testme';
        |DECLARE y = 42;
        |""".stripMargin
    val tree = buildTree(batch)
    // We will automatically add drop statements at the end.
    assert(tree.statements.length == 4)
    val stmt1 = tree.statements.head.asInstanceOf[CiglaVarDeclareStatement]
    val stmt2 = tree.statements(1).asInstanceOf[CiglaVarDeclareStatement]
    assert(stmt1.varName == "x")
    assert(stmt1.command == "DECLARE x = 'testme'")

    assert(stmt2.varName == "y")
    assert(stmt2.command == "DECLARE y = 42")
    val stmt3 = tree.statements(2).asInstanceOf[SparkStatement]
    val stmt4 = tree.statements(3).asInstanceOf[SparkStatement]
    assert(stmt3.command == "DROP TEMPORARY VARIABLE y")
    assert(stmt4.command == "DROP TEMPORARY VARIABLE x")
  }
}