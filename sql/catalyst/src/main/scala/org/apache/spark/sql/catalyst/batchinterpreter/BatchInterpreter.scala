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


trait ProceduralLangInterface {
  def buildInterpreter(
    batch: String,
    evaluator: StatementBooleanEvaluator,
    sparkStatementParser: ParserInterface): ProceduralLangInterpreter
}

case class CiglaLangDispatcher() extends ProceduralLangInterface {
  def buildInterpreter(
    batch: String,
    evaluator: StatementBooleanEvaluator,
    sparkStatementParser: ParserInterface): ProceduralLangInterpreter
  = CiglaLangInterpreter(batch, evaluator, sparkStatementParser)
}

trait ProceduralLangInterpreter extends Iterator[BatchStatementExec]

case class CiglaLangInterpreter(
                                 batch: String,
                                 evaluator: StatementBooleanEvaluator,
                                 sparkStatementParser: ParserInterface)
  extends ProceduralLangInterpreter {
  private val treeNoEval = sparkStatementParser.parseBatch(batch)

  private def getDeclareVarNameFromPlan(
      plan: LogicalPlan): Option[UnresolvedIdentifier] = plan match {
    case CreateVariable(name, _, _) => name match {
      case u: UnresolvedIdentifier => Some(u)
    }
    case _ => None
  }

  private def transformTreeIntoEvaluable(node: BatchPlanStatement): BatchStatementExec = {
    // Set evaluator where needed.
    node match {
      case body: BatchBody =>
        // Find all variables in this scope
        val variables = body.collection.flatMap {
          case st: SparkStatementWithPlan => getDeclareVarNameFromPlan(st.parsedPlan)
          case _ => None
        }
        val dropVars = variables.map(varName => DropVariable(varName, ifExists = true))
          .map(SparkStatementWithPlanExec(_, 0, 0, internal = true)).reverse
        new BatchBodyExec(body.collection.map(stmt => transformTreeIntoEvaluable(stmt)) ++ dropVars)
      case BatchWhileStatement(condition, body) =>
        BatchWhileStatementExec(
          SparkStatementWithPlanExec(
            condition.parsedPlan, condition.sourceStart, condition.sourceEnd, internal = false),
          transformTreeIntoEvaluable(body).asInstanceOf[BatchBodyExec],
          Some(evaluator))
      case BatchIfElseStatement(condition, ifBody, elseBody) =>
        BatchIfElseStatementExec(
          SparkStatementWithPlanExec(
            condition.parsedPlan, condition.sourceStart, condition.sourceEnd, internal = false),
          transformTreeIntoEvaluable(ifBody).asInstanceOf[BatchBodyExec], // TODO deal with this.
          elseBody.map(transformTreeIntoEvaluable(_)).asInstanceOf[Option[BatchBodyExec]],
          Some(evaluator))
      case node: SparkStatementWithPlan =>
        SparkStatementWithPlanExec(
          node.parsedPlan, node.sourceStart, node.sourceEnd, internal = false)
      case _ => throw new IllegalStateException("Unknown statement type")
    }
  }

  private val tree = transformTreeIntoEvaluable(treeNoEval).asInstanceOf[BatchBodyExec]

  private val iter = new BatchBodyExec(tree.collection)
  override def hasNext: Boolean = iter.hasNext
  override def next(): BatchStatementExec = iter.next()
}
