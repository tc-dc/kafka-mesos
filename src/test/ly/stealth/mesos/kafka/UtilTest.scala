/**
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

package ly.stealth.mesos.kafka

import org.junit.Test
import org.junit.Assert._
import java.util
import scala.collection.mutable

class UtilTest {
  // BindAddress
  @Test
  def BindAddress_init {
    new BindAddress("broker0")
    new BindAddress("192.168.*")
    new BindAddress("if:eth1")

    // unknown source
    try { new BindAddress("unknown:value"); fail() }
    catch { case e: IllegalArgumentException => }
  }

  @Test
  def BindAddress_resolve {
    // address without mask
    assertEquals("host", new BindAddress("host").resolve())

    // address with mask
    assertEquals("127.0.0.1", new BindAddress("127.0.0.*").resolve())

    // unresolvable
    try { new BindAddress("255.255.*").resolve(); fail() }
    catch { case e: IllegalStateException => }
  }

  @Test
  def RangeSet_isSuperset = {
    var r1 = Range(1, 10)
    var r2 = Range(2, 3)

    assertTrue(RangeSet.isSuperset(r1, r2))
    assertFalse(RangeSet.isSuperset(r2, r1))

    r2 = Range(2, 11)
    assertFalse(RangeSet.isSuperset(r1, r2))

    r2 = Range(12, 15)
    assertFalse(RangeSet.isSuperset(r1, r2))
    assertFalse(RangeSet.isSuperset(r2, r1))
  }

  @Test
  def RangeSet_overlap = {
    var r1 = Range(1, 10)
    var r2 = Range(3, 12)

    assertTrue(RangeSet.overlap(r1, r2))

    r2 = Range(12, 15)
    assertFalse(RangeSet.overlap(r1, r2))
  }

  @Test
  def RangedSet_unionRanges = {
    // superset
    var r1 = Range(1, 10)
    var r2 = Range(2, 4)

    assertEquals(Range(1, 10).toSet, RangeSet.unionRanges(r1, r2).flatten.toSet)
    // no intersection
    r2 = Range(12, 14)
    assertEquals(Seq(Range(1, 10), Range(12,14)).flatten.toSet, RangeSet.unionRanges(r1, r2).flatten.toSet)

    // overlap
    r2 = Range(5, 15)
    assertEquals(Range(1, 15).toSet, RangeSet.unionRanges(r1, r2).flatten.toSet)
  }

  @Test
  def RangeSet_union = {
    var r1 = new RangeSet(Range(1, 10))
    var r2 = new RangeSet(mutable.ArrayBuffer(Range(2, 4)))

    assertEquals(Range(1, 10).toSet, r1 | r2)

    r2 = new RangeSet(Seq(Range(2, 15), Range(1, 2), Range(20, 30)))
    assertEquals((Range(1, 15) ++ Range(20, 30)).toSet, r1 | r2)

    r2 = new RangeSet(Seq(Range(-1, 5), Range(6, 15), Range(20, 30)))
    assertEquals((Range(-1, 15) ++ Range(20, 30)).toSet, r1 | r2)

    r1 = new RangeSet(Seq(Range(1, 10), Range(20, 25)))
    assertEquals((Range(-1, 15) ++ Range(20, 30)).toSet, r1 | r2)
  }


  @Test
  def RangeSet_intersect = {
    var r1 = new RangeSet(Range(1, 10))
    var r2 = new RangeSet(Seq(Range(2, 4), Range(5, 8)))

    assertEquals((Range(2, 4) ++ Range(5, 8)).toSet, r1 & r2)

    // no intersection
    r2 = new RangeSet(Range(20, 30))
    assertEquals(Set(), r1 & r2)
  }
}
