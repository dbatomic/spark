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

package org.apache.spark.sql.catalyst.batchinterpreter

import org.apache.spark.sql.catalyst.analysis.UnresolvedIdentifier
import org.apache.spark.sql.catalyst.parser.{BatchBody, BatchIfElseStatement, BatchPlanStatement, BatchWhileStatement, ParserInterface, SparkStatementWithPlan}
import org.apache.spark.sql.catalyst.plans.logical.{CreateVariable, DropVariable, LogicalPlan}

trait ProceduralLangInterpreter {
  def buildExecutionPlan(
    batch: String, evaluator: StatementBooleanEvaluator) : Iterator[BatchStatementExec]
}

case class CiglaLangInterpreter(sparkStatementParser: ParserInterface)
  extends ProceduralLangInterpreter {

  def buildExecutionPlan(
      batch: String,
      evaluator: StatementBooleanEvaluator): Iterator[BatchStatementExec] = {
    val treeNoEval = sparkStatementParser.parseBatch(batch)

    val tree = transformTreeIntoEvaluable(treeNoEval, evaluator).asInstanceOf[BatchBodyExec]
    new BatchBodyExec(tree.collection)
  }

  private def getDeclareVarNameFromPlan(
      plan: LogicalPlan): Option[UnresolvedIdentifier] = plan match {
    case CreateVariable(name, _, _) => name match {
      case u: UnresolvedIdentifier => Some(u)
    }
    case _ => None
  }

  private def transformTreeIntoEvaluable(
      node: BatchPlanStatement, evaluator: StatementBooleanEvaluator): BatchStatementExec = {
    node match {
      case body: BatchBody =>
        // Find all variables in this scope
        val variables = body.collection.flatMap {
          case st: SparkStatementWithPlan => getDeclareVarNameFromPlan(st.parsedPlan)
          case _ => None
        }
        val dropVars = variables.map(varName => DropVariable(varName, ifExists = true))
          .map(SparkStatementWithPlanExec(_, 0, 0, internal = true)).reverse
        new BatchBodyExec(
          body.collection.map(stmt => transformTreeIntoEvaluable(stmt, evaluator)) ++ dropVars)
      case BatchWhileStatement(condition, body) =>
        BatchWhileStatementExec(
          SparkStatementWithPlanExec(
            condition.parsedPlan, condition.sourceStart, condition.sourceEnd, internal = false),
          transformTreeIntoEvaluable(body, evaluator).asInstanceOf[BatchBodyExec],
          Some(evaluator))
      case BatchIfElseStatement(condition, ifBody, elseBody) =>
        BatchIfElseStatementExec(
          SparkStatementWithPlanExec(
            condition.parsedPlan, condition.sourceStart, condition.sourceEnd, internal = false),
          transformTreeIntoEvaluable(ifBody, evaluator).asInstanceOf[BatchBodyExec],
          elseBody.map(
            transformTreeIntoEvaluable(_, evaluator)).asInstanceOf[Option[BatchBodyExec]],
          Some(evaluator))
      case node: SparkStatementWithPlan =>
        SparkStatementWithPlanExec(
          node.parsedPlan, node.sourceStart, node.sourceEnd, internal = false)
    }
  }
}
