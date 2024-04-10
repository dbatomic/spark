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

package org.apache.spark.sql.batchinterpreter

import org.apache.spark.SparkFunSuite

import org.apache.spark.sql.batchinterpreter.{BatchNestedIteratorStatementExec, BatchStatementExec, BatchWhileStatementExec, LeafStatementExec, StatementBooleanEvaluator}

class BatchStatementsSuite extends SparkFunSuite {
  case class TestStatement(myval: String) extends LeafStatementExec {
    override def rewind(): Unit = ()
  }

  class TestBody(stmts: List[BatchStatementExec])
    extends BatchNestedIteratorStatementExec(stmts)

  // Return false every reps-th time.
  case class RepEval(reps: Int) extends StatementBooleanEvaluator {
    var callCount = 0
    override def eval(statement: LeafStatementExec): Boolean = {
      callCount += 1
      !(callCount % (reps + 1) == 0)
    }
  }

  class TestWhile(condition: LeafStatementExec, body: TestBody, reps: Int)
    extends BatchWhileStatementExec(condition, body, Some(RepEval(reps)))

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
      case _ => fail("Unexpected statement type")
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
