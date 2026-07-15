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
package org.apache.gluten.extension.columnar

import org.apache.gluten.execution.{SortExecTransformer, TransformSupport}
import org.apache.gluten.sql.shims.SparkShimLoader

import org.apache.spark.sql.catalyst.expressions.{Expression, SortOrder}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ColumnarWriteFilesExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.WriteFilesExec

/**
 * Re-adds the local sort on dynamic partition (and bucket) columns that a row-based
 * [[WriteFilesExec]] relies on, when Gluten has removed it while offloading the upstream
 * `SortAggregate` to a native hash aggregate (see [[EliminateLocalSort]]).
 *
 * Background:
 *   - With planned write enabled, `V1Writes` drops the write's required sort at logical
 *     optimization time when the child already provides the ordering (e.g. a `SortAggregate` keyed
 *     by the partition column produces output already ordered by that column). Hence the physical
 *     `WriteFilesExec.requiredChildOrdering` is `Nil`.
 *   - Gluten then replaces the `SortAggregate` with an (unordered) native hash aggregate and
 *     [[EliminateLocalSort]] eagerly removes the now-redundant local sort feeding it.
 *   - The ordering that the write implicitly depended on is thereby destroyed. When the native
 *     Velox write is not applicable (e.g. ORC/Hive tables) the write falls back to the row-based
 *     `WriteFilesExec`, whose `DynamicPartitionDataSingleWriter` requires the rows to be sorted by
 *     the partition columns. Unsorted rows make it re-create an already-closed partition file and
 *     fail with `FileAlreadyExistsException`.
 *
 * Native Velox writes ([[org.apache.gluten.execution.WriteFilesExecTransformer]]) do not need the
 * ordering, so this rule only patches the row-based fallback write. The dummy no-op
 * `WriteFilesExec` that [[ColumnarWriteFilesExec]] injects for the native path is skipped by
 * checking for its [[ColumnarWriteFilesExec.NoopLeaf]] child.
 *
 * The required ordering is obtained through [[SparkShimLoader]] from `V1WritesUtils.getSortOrder`
 * so that the behavior stays identical to vanilla `FileFormatWriter` (including the
 * concurrent-writers and small-file-merge cases where `getSortOrder` returns empty). It is fetched
 * via the shim because `V1WritesUtils` only exists since Spark 3.4; on Spark 3.2/3.3 the shim
 * returns `Nil` and this rule is a no-op (the planned-write `WriteFilesExec` path is not used
 * there). The "already satisfied" check mirrors `V1WritesUtils.isOrderingMatched`, which is pure
 * catalyst and version-agnostic, so it is inlined here.
 *
 * This rule must run before [[org.apache.gluten.extension.columnar.transition.InsertTransitions]]:
 * when the write's child is an offloaded transformer we insert a native [[SortExecTransformer]]
 * (validated, falling back to a row-based [[SortExec]]) so the sort runs columnar in Velox, and
 * InsertTransitions afterwards places the ColumnarToRow transition above the sort.
 */
object EnsureRowBasedWriteFilesOrdering extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = plan.transformUp {
    case w: WriteFilesExec if !w.child.isInstanceOf[ColumnarWriteFilesExec.NoopLeaf] =>
      val requiredOrdering: Seq[SortOrder] =
        SparkShimLoader.getSparkShims.getWriteFilesRequiredOrdering(
          w.child.output,
          w.partitionColumns,
          w.bucketSpec,
          w.options,
          w.staticPartitions.size)
      if (
        requiredOrdering.isEmpty ||
        isOrderingMatched(requiredOrdering.map(_.child), w.child.outputOrdering)
      ) {
        w
      } else {
        w.withNewChildren(sortPlan(w.child, requiredOrdering) :: Nil)
      }
  }

  // Mirrors `V1WritesUtils.isOrderingMatched`. Pure catalyst and identical across Spark versions,
  // so it is inlined to avoid depending on `V1WritesUtils`, which is absent before Spark 3.4.
  private def isOrderingMatched(
      requiredOrdering: Seq[Expression],
      outputOrdering: Seq[SortOrder]): Boolean = {
    if (requiredOrdering.length > outputOrdering.length) {
      false
    } else {
      requiredOrdering.zip(outputOrdering).forall {
        case (requiredOrder, outputOrder) =>
          outputOrder.satisfies(outputOrder.copy(child = requiredOrder))
      }
    }
  }

  private def sortPlan(child: SparkPlan, requiredOrdering: Seq[SortOrder]): SparkPlan =
    child match {
      case c: TransformSupport =>
        val nativeSort = SortExecTransformer(requiredOrdering, global = false, c)
        if (nativeSort.doValidate().ok()) {
          nativeSort
        } else {
          SortExec(requiredOrdering, global = false, c)
        }
      case other =>
        SortExec(requiredOrdering, global = false, other)
    }
}
