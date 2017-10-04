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
package org.apache.spark.rdd

import org.scalatest.FunSpec

import org.apache.spark.SharedSparkContext

class RDDFunSpec extends FunSpec with SharedSparkContext {
  describe("An RDD") {
    it("have the specified number of partitions") {
      assert(1 === sc.makeRDD(Array(1, 2, 3, 4), 1).getNumPartitions)
      assert(2 === sc.makeRDD(Array(1, 2, 3, 4), 2).getNumPartitions)
      assert(3 === sc.makeRDD(Array(1, 2, 3, 4), 3).getNumPartitions)
      assert(4 === sc.makeRDD(Array(1, 2, 3, 4), 4).getNumPartitions)
    }

    it("have the correct element count, no matter how many partitions it has") {
      assert(4 === sc.makeRDD(Array(1, 2, 3, 4), 1).count)
      assert(4 === sc.makeRDD(Array(1, 2, 3, 4), 2).count)
      assert(4 === sc.makeRDD(Array(1, 2, 3, 4), 3).count)
      assert(4 === sc.makeRDD(Array(1, 2, 3, 4), 4).count)
    }
  }
  describe("An RDD with duplicate elements") {
    it("eliminate duplicates with distinct") {
      assert(4 === sc.makeRDD(Array(1, 1, 2, 2, 3, 3, 4, 4), 1).distinct.count)
      assert(4 === sc.makeRDD(Array(1, 1, 2, 2, 3, 3, 4, 4), 2).distinct.count)
      assert(4 === sc.makeRDD(Array(1, 1, 2, 2, 3, 3, 4, 4), 3).distinct.count)
      assert(4 === sc.makeRDD(Array(1, 1, 2, 2, 3, 3, 4, 4), 4).distinct.count)
    }
  }
}
