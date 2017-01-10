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

import java.util
import scala.collection.JavaConversions._
import java.io.{File, IOException}
import java.net.{Inet4Address, InetAddress, NetworkInterface}
import java.util.Date
import scala.collection.{GenSet, mutable}

trait Clock {
  def now(): Date
}

trait ClockComponent {
  val clock: Clock
}

trait WallClockComponent extends ClockComponent {
  val clock: Clock = new WallClock

  class WallClock extends Clock {
    def now(): Date = new Date()
  }
}

object RunnableConversions {
  implicit def fnToRunnable[T](fn: () => T): Runnable = new Runnable {
    override def run(): Unit = fn()
  }
}

object RangeSet {
  def intersectRanges(r1: Range, r2: Range): Option[Range] = {
    // 0..2 | 3..4
    if (!(
      r2.contains(r1.start)
        || r2.contains(r1.end)
        || r1.contains(r2.start)
        || r1.contains(r2.end))) {
      // No overlap
      None
    }
    else {
      val start = r1.start.max(r2.start)
      val end = r1.end.min(r2.end)
      val (_, bigger) = if (r1.end > r2.end) (r1, r2) else (r2, r1)

      if (bigger.isInclusive)
        Some(Range.inclusive(start, end))
      else
        Some(Range(start, end))
    }
  }

  def isSuperset(r1: Range, r2: Range) = r1.contains(r2.start) && r1.contains(r2.end)

  def overlap(r1: Range, r2: Range) = (r1.contains(r2.start)
    || r1.contains(r2.end)
    || r2.contains(r1.start)
    || r2.contains(r1.end))

  def unionRanges(r1: Range, r2: Range): Seq[Range] = {
    if (isSuperset(r1, r2))
      Seq(r1)
    else if (isSuperset(r2, r1))
      Seq(r2)
    else {
      if (!overlap(r1, r2))
        Seq(r1, r2)
      else {
        val (_, bigger) = if (r1.end > r2.end) (r2, r1) else (r1, r2)
        val start = r1.start.min(r2.start)
        Seq(if (bigger.isInclusive)
          Range.inclusive(start, bigger.end)
        else
          Range(start, bigger.end))
      }
    }
  }
}

class RangeSet(private val ranges: Seq[Range] = Seq()) extends Set[Int] {
  def this(r: Range) = this(Seq(r))

  private lazy val sortedRanges = ranges.sortBy(_.start)

  override def contains(elem: Int): Boolean = ranges.exists(_.contains(elem))

  override def +(elem: Int): Set[Int] = {
    if (contains(elem))
      this
    else
      new RangeSet(ranges ++ Seq(Range.inclusive(elem, elem)))
  }

  override def -(elem: Int): Set[Int] = {
    ranges.find(_.contains(elem)).map { r =>
      val newRanges = ranges.filterNot(_.eq(r)) ++ Seq(
        Range(r.start, elem),
        if (r.isInclusive) Range.inclusive(elem + 1, r.end) else Range(elem + 1, r.end)
      )
      new RangeSet(newRanges)
    }.getOrElse(this)
  }

  override def union(that: GenSet[Int]): Set[Int] = that match {
    case other: RangeSet =>
      val buffer = mutable.Buffer[Range]()
      var allRanges = (other.ranges ++ ranges).sortBy(_.start)

      while(allRanges.length > 1) {
        allRanges match {
          case r1 +: r2 +: rest =>
            if (!RangeSet.overlap(r1, r2)) {
              buffer.append(r1)
              allRanges = Seq(r2) ++ rest
            } else {
              allRanges = RangeSet.unionRanges(r1, r2) ++ rest
            }
        }
      }
      new RangeSet(allRanges ++ buffer)
    case _ => super.union(that)
  }

  override def intersect(that: GenSet[Int]): Set[Int] = that match {
    case other: RangeSet =>
      val overlaps = other.ranges.flatMap {
        o => ranges.flatMap { r => RangeSet.intersectRanges(o, r) }
      }
      new RangeSet(overlaps)
    case _ => super.intersect(that)
  }

  override def iterator: Iterator[Int] = sortedRanges.flatMap(_.toIterator).toIterator
}

class BindAddress(s: String) {
  private def parse() = {
    val idx = s.indexOf(":")
    val (source, value) =
      if (idx != -1) {
        (s.substring(0, idx), s.substring(idx + 1))
      } else
        (null, s)

    if (source != null && source != "if")
      throw new IllegalArgumentException(s)

    (source, value)
  }

  val (source, value) = parse()

  def resolve(): String = {
    source match {
      case null => resolveAddress(value)
      case "if" => resolveInterfaceAddress(value)
      case _ => throw new IllegalStateException("Failed to resolve " + s)
    }
  }

  def resolveAddress(addressOrMask: String): String = {
    if (!addressOrMask.endsWith("*")) return addressOrMask
    val prefix = addressOrMask.substring(0, addressOrMask.length - 1)

    NetworkInterface.getNetworkInterfaces
      .flatMap(_.getInetAddresses)
      .find(_.getHostAddress.startsWith(prefix))
      .map(_.getHostAddress)
      .getOrElse(throw new IllegalStateException("Failed to resolve " + s))
  }

  def resolveInterfaceAddress(name: String): String =
    NetworkInterface.getNetworkInterfaces
      .find(_.getName == name)
      .flatMap { iface => iface.getInetAddresses
        .find(_.isInstanceOf[Inet4Address])
        .map(_.getHostAddress)
      }
      .getOrElse(throw new IllegalStateException("Failed to resolve " + s))

  override def hashCode(): Int = 31 * source.hashCode + value.hashCode

  override def equals(o: scala.Any): Boolean = {
    if (!o.isInstanceOf[BindAddress]) return false
    val address = o.asInstanceOf[BindAddress]
    source == address.source && value == address.value
  }

  override def toString: String = s
}

object Util {
  var terminalWidth: Int = getTerminalWidth
  private def getTerminalWidth: Int = {
    // No tty, don't bother trying to find the terminal width
    if (System.console() == null) {
      return 80
    }

    val file: File = File.createTempFile("getTerminalWidth", null)
    file.delete()

    var width = 80
    try {
      new ProcessBuilder(List("bash", "-c", "tput cols"))
        .inheritIO().redirectOutput(file).start().waitFor()

      val source = scala.io.Source.fromFile(file)
      width = try Integer.valueOf(source.mkString.trim) finally source.close()
    } catch {
      case e: IOException => /* ignore */
      case e: NumberFormatException => /* ignore */
    }

    file.delete()
    width
  }

  def readLastLines(file: File, n: Int, maxBytes: Int = 102400): String = {
    require(n > 0)

    val raf = new java.io.RandomAccessFile(file, "r")
    val fileLength: Long = raf.length()
    var line = 0
    var pos: Long = fileLength - 1
    var found = false
    var nlPos = pos

    try {
      while (pos != -1 && !found) {
        raf.seek(pos)

        if (raf.readByte() == 10) {
          if (pos != fileLength - 1) {
            line += 1
          }
          nlPos = pos
        }

        if (line == n) found = true
        if (fileLength - pos > maxBytes) found = true

        if (!found) pos -= 1
      }

      if (pos == -1) pos = 0

      if (line == n) {
        pos = pos + 1
      } else if (fileLength - pos > maxBytes) {
        pos = nlPos + 1
      }

      raf.seek(pos)

      val buffer = new Array[Byte]((fileLength - pos).toInt)

      raf.read(buffer, 0, buffer.length)

      val str = new String(buffer, "UTF-8")

      str
    } finally {
      raf.close()
    }
  }
}
