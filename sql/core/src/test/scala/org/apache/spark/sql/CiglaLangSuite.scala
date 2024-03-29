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

package org.apache.spark.sql

import org.apache.spark.SparkFunSuite

import org.apache.spark.sql.catalyst.parser.{BoolEvaluableStatement, CiglaLangBuilder, CiglaLangNestedIteratorStatement, CiglaVarDeclareStatement, CiglaWhileStatement, LeafStatement, SparkStatement, StatementBooleanEvaluator}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.test.SharedSparkSession

class CiglaLangSuite extends SparkFunSuite {
  // mocks...
  case class TestStatement(myval: String) extends LeafStatement with BoolEvaluableStatement {
    override def rewind(): Unit = ()
  }

  class TestBody(stmts: List[CiglaLangBuilder.CiglaLanguageStatement])
    extends CiglaLangNestedIteratorStatement(stmts)

  // Return false every reps-th time.
  case class RepEval(reps: Int) extends StatementBooleanEvaluator {
    var callCount = 0
    override def eval(statement: BoolEvaluableStatement): Boolean = {
      callCount += 1
      !(callCount % (reps + 1) == 0)
    }
  }

  class TestWhile(condition: BoolEvaluableStatement, body: TestBody, reps: Int)
    extends CiglaWhileStatement(condition, body, RepEval(reps))

  test("test body single statement") {
    val nestedIterator = new TestBody(
      List(TestStatement("one")))
    val statements = nestedIterator.map {
      case stmt: TestStatement => stmt.myval
      case _ => fail("Unexpected statement type")
    }.toList

    assert(statements === List("one"))
  }

  test("test body no nesting") {
    val nestedIterator = new TestBody(
      List(TestStatement("one"), TestStatement("two"), TestStatement("three")))
    val statements = nestedIterator.map {
      case TestStatement(v) => v
    }.toList

    assert(statements === List("one", "two", "three"))
  }

  test("test body in body") {
    val nestedIterator = new TestBody(List(
      new TestBody(List(TestStatement("one"), TestStatement("two"))),
      TestStatement("three"),
      new TestBody(List(TestStatement("four"), TestStatement("five")))))

    val statements = nestedIterator.map {
      case stmt: TestStatement => stmt.myval
      case _ => fail("Unexpected statement type")
    }.toList

    assert(statements === List("one", "two", "three", "four", "five"))
  }

  test("test while loop") {
    val iter = new TestBody(List(
      new TestWhile(
        TestStatement("condition"),
        new TestBody(List(TestStatement("42"))), 3)
    ))
    val statements = iter.map {
      case stmt: TestStatement => stmt.myval
      case _ => fail("Unexpected statement type")
    }.toList
    assert(statements === List(
      "condition", "42", "condition", "42", "condition", "42", "condition"))
  }

  test("nested while loop") {
    // inner 2x, outer 3x
    val iter = new TestBody(List(
      new TestWhile(
        TestStatement("con1"),
        new TestBody(List(
          TestStatement("42"),
          new TestWhile(
            TestStatement("con2"),
              new TestBody(List(TestStatement("43"))), 2)
        )), 3)
    ))
    val statements = iter.map {
      case stmt: TestStatement => stmt.myval
      case _ => fail("Unexpected statement type")
    }.toList
    assert(statements === List(
      "con1", "42", "con2", "43", "con2", "43", "con2",
      "con1", "42", "con2", "43", "con2", "43", "con2",
      "con1", "42", "con2", "43", "con2", "43", "con2",
      "con1"
    ))
  }
}

//noinspection ScalaStyle
class CiglaLangSuiteE2E extends QueryTest with SharedSparkSession {
  private def verifyBatchResult(
      batch: String, expected: Seq[Seq[Row]], printRes: Boolean = false): Unit = {
    val commands = sqlBatch(batch)
    val result = commands.flatMap {
      case stmt: SparkStatement =>
        if (printRes) {
          println("Executing: " + stmt.command)
        }

        // If expression will be executed on interpreter side.
        // We need to see what kind of behaviour we want to get here...
        // Example here is expression in while loop/if/else.
        if (stmt.consumed) {
          None
        } else {
          Some(Dataset.ofRows(spark, stmt.parsedPlan, new QueryPlanningTracker))
        }
      case stmt: CiglaVarDeclareStatement =>
        if (printRes) {
          println("Executing: " + stmt.command)
        }
        Some(Dataset.ofRows(spark, stmt.parsedPlan, new QueryPlanningTracker))
      case _ => None
    }.toArray

    assert(result.length == expected.size)
    result.zip(expected).foreach { case (df, expected) => checkAnswer(df, expected) }
  }

  test("select 1") {
    verifyBatchResult("SELECT 1;", Seq(Seq(Row(1))))
  }

  test("simple multistatement") {
    withTable("t") {
      val commands = """
        |CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
        |INSERT INTO t VALUES (1, 'a', 1.0);
        |SELECT a, b FROM T WHERE a=12;
        |SELECT a FROM t;
        |SET x = 12;
        |""".stripMargin

      val expected: Seq[Seq[Row]] = Seq(
        Seq.empty[Row], // For create table
        Seq.empty[Row], // For insert
        Seq.empty[Row], // For select with filter
        Seq(Row(1)), // For select
        Seq(Row("x", "12"))) // For set

      verifyBatchResult(commands, expected)
    }
  }

  test("count multistatement") {
    withTable("t") {
      val commands = """
        |CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
        |INSERT INTO t VALUES (1, 'a', 1.0);
        |INSERT INTO t VALUES (1, 'a', 1.0);
        |SELECT CASE WHEN COUNT(*) > 10 THEN true
        |ELSE false
        |END as MoreThanTen
        |FROM t;
        |""".stripMargin

      val expected = Seq(Seq.empty[Row], Seq.empty[Row], Seq.empty[Row], Seq(Row(false)))
      verifyBatchResult(commands, expected)
    }
  }

  test("if") {
    val commands = """
      | IF SELECT TRUE;
      | THEN
      |   SELECT 42;
      | END IF;
      |""".stripMargin
    val expected = Seq(Seq(Row(42)))
    verifyBatchResult(commands, expected)
  }

  test("if else going in if") {
    val commands = """
      | IF SELECT TRUE;
      | THEN
      |   SELECT 42;
      | ELSE
      |   SELECT 43;
      | END IF;
      |""".stripMargin

    val expected = Seq(Seq(Row(42)))
    verifyBatchResult(commands, expected)
  }

  test("if else going in else") {
    val commands = """
      | IF SELECT FALSE;
      | THEN
      |   SELECT 42;
      | ELSE
      |   SELECT 43;
      | END IF;
      |""".stripMargin

    val expected = Seq(Seq(Row(43)))
    verifyBatchResult(commands, expected)
  }

  test("if with count") {
    withTable("t") {
      val commands = """
        |CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
        |INSERT INTO t VALUES (1, 'a', 1.0);
        |INSERT INTO t VALUES (1, 'a', 1.0);
        |IF SELECT COUNT(*) > 2 FROM t;
        | THEN
        |   SELECT 42;
        | ELSE
        |   SELECT 43;
        | END IF;
        |""".stripMargin

      val expected = Seq(Seq.empty[Row], Seq.empty[Row], Seq.empty[Row], Seq(Row(43)))
      verifyBatchResult(commands, expected)
    }
  }

  test("while") {
    withTable("t") {
      val commands =
        """
          |CREATE TABLE t (a INT, b STRING, c DOUBLE) USING parquet;
          |WHILE SELECT COUNT(*) < 2 FROM t; DO
          |  INSERT INTO t VALUES (1, 'a', 1.0);
          |END WHILE;
          |SELECT COUNT(*) FROM t;
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // Create table
        Seq.empty[Row], // First insert
        Seq.empty[Row], // Second insert
        Seq(Row(2)) // Select count
      )

      verifyBatchResult(commands, expected, printRes = true)
    }
  }

  test("nested while") {
    withTable("t1", "t2") {
      val commands =
        """
          |CREATE TABLE t1 (a INT) USING parquet;
          |CREATE TABLE t2 (a INT) USING parquet;
          |WHILE SELECT COUNT(*) < 2 FROM t1; DO
          |  INSERT INTO t1 VALUES (1);
          |  WHILE SELECT COUNT(*) < 2 FROM t2; DO
          |   INSERT INTO t2 VALUES (1);
          |  END WHILE;
          |  TRUNCATE TABLE t2;
          |END WHILE;
          |SELECT COUNT(*) FROM t1;
          |SELECT COUNT(*) FROM t2;
          |""".stripMargin

      val expected = Seq(
        Seq.empty[Row], // Create table t1
        Seq.empty[Row], // Create table t2
        Seq.empty[Row], // First insert t1
        Seq.empty[Row], // First insert t2
        Seq.empty[Row], // Second insert t2
        Seq.empty[Row], // Truncate t2
        Seq.empty[Row], // Second insert t1
        Seq.empty[Row], // Third insert t2
        Seq.empty[Row], // Forth insert t2
        Seq.empty[Row], // Truncate t2
        Seq(Row(2)), // Select count t1
        Seq(Row(0)), // Select count t2
      )

      verifyBatchResult(commands, expected, printRes = true)
    }
  }

  test("nested while direct exec") {
    withTable("t1", "t2") {
      val commands =
        """
          |CREATE TABLE t1 (a INT) USING parquet;
          |CREATE TABLE t2 (a INT) USING parquet;
          |DECLARE totalInsertCount = 0;
          |WHILE SELECT COUNT(*) < 2 FROM t1; DO
          |  INSERT INTO t1 VALUES (1);
          |  SET VAR totalInsertCount = totalInsertCount + 1;
          |  WHILE SELECT COUNT(*) < 2 FROM t2; DO
          |   INSERT INTO t2 VALUES (1);
          |   SET VAR totalInsertCount = totalInsertCount + 1;
          |   SELECT COUNT(*) as T2Count FROM t2;
          |  END WHILE;
          |  TRUNCATE TABLE t2;
          |END WHILE;
          |SELECT COUNT(*) as t1FinalCount FROM t1;
          |SELECT COUNT(*) as t2FinalCount FROM t2;
          |SELECT totalInsertCount;
          |""".stripMargin
      spark.sqlBatchExec(commands).foreach(_.show())
    }
  }

  test("session variable set and read") {
    val commands =
      """
        |DECLARE var = 1;
        |SET VAR var = var + 1;
        |SELECT var;
        |""".stripMargin
    val expected = Seq(
      Seq.empty[Row], // Declare
      Seq.empty[Row], // SET
      Seq(Row(2)), // Select
    )
    verifyBatchResult(commands, expected, printRes = true)
  }

  // TODO: Tests for proper error reporting...
}
