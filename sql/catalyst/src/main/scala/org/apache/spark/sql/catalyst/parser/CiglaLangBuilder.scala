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

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.CollectionHasAsScala

import org.antlr.v4.runtime.tree.{ErrorNode, ParseTree, RuleNode, TerminalNode}

import org.apache.spark.sql.catalyst.parser.CiglaBaseParserBaseVisitor

case class SingleStatement(command: String)
case class MultiStatement(statements: ArrayBuffer[SingleStatement])

//noinspection ScalaStyle
case class CiglaLangBuilder(batch: String) extends CiglaBaseParserBaseVisitor[AnyRef] {
  override def visitSingleStatement(
      ctx: CiglaBaseParser.SingleStatementContext): SingleStatement = {
    val start = ctx.start.getStartIndex
    val stop = ctx.stop.getStopIndex
    val command = batch.substring(start, stop + 1)
    SingleStatement(command)
  }

  override def visitMultiStatement(ctx: CiglaBaseParser.MultiStatementContext): MultiStatement = {
    val stmts = ctx.singleStatement()
    MultiStatement(stmts.asScala.map(visitSingleStatement)
      .asInstanceOf[ArrayBuffer[SingleStatement]])
  }

  override def visit(parseTree: ParseTree): AnyRef = ???

  override def visitChildren(ruleNode: RuleNode): AnyRef = ???

  override def visitTerminal(terminalNode: TerminalNode): AnyRef = ???

  override def visitErrorNode(errorNode: ErrorNode): AnyRef = ???

  override def visitStatementBody(ctx: CiglaBaseParser.StatementBodyContext): AnyRef = ???

  override def visitStringLitOrIdentifierOrConstant(
    ctx: CiglaBaseParser.StringLitOrIdentifierOrConstantContext): AnyRef = ??? }