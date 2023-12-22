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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class Collate(inputString: Expression, collation: Expression)
  extends BinaryExpression with CodegenFallback with ImplicitCastInputTypes {
  override def left: Expression = inputString
  override def right: Expression = collation

  @transient
  private lazy val collationEval = right.eval().asInstanceOf[UTF8String]

  override def dataType: DataType = StringType(collationEval.toString)

  // TODO: For now collate accepts only "utf8" default collation.
  // It should work from other collations as well.
  override def inputTypes: Seq[AbstractDataType] = Seq(StringType("utf8"), StringType("utf8"))

  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): Expression = copy(newLeft, newRight)

  // Just pass through.
  override def eval(input: InternalRow): Any = left.eval(input)
}

case class Collation(child: Expression) extends UnaryExpression with CodegenFallback {
  /**
   * Returns the [[DataType]] of the result of evaluating this expression.  It is
   * invalid to query the dataType of an unresolved expression (i.e., when `resolved` == false).
   */
  override def dataType: DataType = StringType("utf8")

  override protected def withNewChildInternal(newChild: Expression): Expression = copy(newChild)

  override def eval(input: InternalRow): Any = child.dataType match {
    case StringType(collation) => UTF8String.fromString(collation)
    case _ => throw new IllegalArgumentException("Collation expects StringType")
  }
}