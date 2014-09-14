/*
 * =========================================================================================
 * Copyright © 2013-2014 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package kamon.csvreporter

import java.io.{ FileWriter, File }
import java.text.{ DecimalFormat, SimpleDateFormat }
import java.util.Date

import akka.actor._
import akka.event.Logging
import kamon.Kamon
import kamon.metric.ActorMetrics.ActorMetricSnapshot
import kamon.metric.Subscriptions.TickMetricSnapshot
import kamon.metric.TraceMetrics.TraceMetricsSnapshot
import kamon.metric.UserMetrics._
import kamon.metric._
import kamon.metric.instrument.{ Counter, Histogram }
import kamon.metrics.GCMetrics.GCMetricSnapshot
import kamon.metrics.MemoryMetrics.MemoryMetricSnapshot
import kamon.metrics.NetworkMetrics.NetworkMetricSnapshot
import kamon.metrics.ProcessCPUMetrics.ProcessCPUMetricsSnapshot
import kamon.metrics._
import kamon.metrics.CPUMetrics.CPUMetricSnapshot

object CsvReporter extends ExtensionId[CsvReporterExtension] with ExtensionIdProvider {
  override def lookup(): ExtensionId[_ <: Extension] = CsvReporter
  override def createExtension(system: ExtendedActorSystem): CsvReporterExtension = new CsvReporterExtension(system)

  trait MetricKeyGenerator {
    def localhostName: String
    def normalizedLocalhostName: String
    def generateKey(groupIdentity: MetricGroupIdentity, metricIdentity: MetricIdentity): String
  }
}

class CsvReporterExtension(system: ExtendedActorSystem) extends Kamon.Extension {
  val log = Logging(system, classOf[CsvReporterExtension])
  log.info("Starting the Kamon(CsvReporter) extension")

  val csvReporterConfig = system.settings.config.getConfig("kamon.csv-reporter")

  val subscriber = system.actorOf(Props[CsvReporterSubscriber], "kamon-csv-reporter")
  Kamon(Metrics)(system).subscribe(TraceMetrics, "*", subscriber, permanently = true)
  Kamon(Metrics)(system).subscribe(ActorMetrics, "*", subscriber, permanently = true)

  // Subscribe to all user metrics
  Kamon(Metrics)(system).subscribe(UserHistograms, "*", subscriber, permanently = true)
  Kamon(Metrics)(system).subscribe(UserCounters, "*", subscriber, permanently = true)
  Kamon(Metrics)(system).subscribe(UserMinMaxCounters, "*", subscriber, permanently = true)
  Kamon(Metrics)(system).subscribe(UserGauges, "*", subscriber, permanently = true)

  val includeSystemMetrics = csvReporterConfig.getBoolean("report-system-metrics")

  if (includeSystemMetrics) {
    // Subscribe to SystemMetrics
    Kamon(Metrics)(system).subscribe(CPUMetrics, "*", subscriber, permanently = true)
    Kamon(Metrics)(system).subscribe(ProcessCPUMetrics, "*", subscriber, permanently = true)
    Kamon(Metrics)(system).subscribe(NetworkMetrics, "*", subscriber, permanently = true)
  }
}

class CsvReporterSubscriber extends Actor with ActorLogging {
  private val csvConfig = context.system.settings.config.getConfig("kamon.csv-reporter")
  private val csvFile = new File(csvConfig.getString("csv-file"))

  import kamon.csvreporter.CsvReporterSubscriber.RichHistogramSnapshot

  def receive = {
    case tick: TickMetricSnapshot ⇒ printMetricSnapshot(tick)
  }

  def printMetricSnapshot(tick: TickMetricSnapshot): Unit = {
    val csvLines = tick.metrics flatMap {
      case (identity, ams: ActorMetricSnapshot)                 ⇒ createActorMetrics(identity.name, ams)
      //      case (identity, tms: TraceMetricsSnapshot)                ⇒ logTraceMetrics(identity.name, tms)
      case (h: UserHistogram, s: UserHistogramSnapshot)         ⇒ CsvMetricsLine(h.name, s.histogramSnapshot) :: Nil
      case (c: UserCounter, s: UserCounterSnapshot)             ⇒ CsvMetricsLine(c.name, s.counterSnapshot) :: Nil
      case (m: UserMinMaxCounter, s: UserMinMaxCounterSnapshot) ⇒ CsvMetricsLine(m.name, s.minMaxCounterSnapshot) :: Nil
      case (g: UserGauge, s: UserGaugeSnapshot)                 ⇒ CsvMetricsLine(g.name, s.gaugeSnapshot) :: Nil
      case (_, cms: CPUMetricSnapshot)                          ⇒ createCpuMetrics(cms)
      case (_, pcms: ProcessCPUMetricsSnapshot)                 ⇒ createProcessCpuMetrics(pcms)
      case (_, nms: NetworkMetricSnapshot)                      ⇒ createNetworkMetrics(nms)
      case ignoreEverythingElse                                 ⇒ Nil
    }

    val writeHeader = !csvFile.exists()
    val fout = new FileWriter(csvFile, true)
    if (writeHeader) {
      fout.write("Timestamp" + CsvMetricsLine.Separator + " " + CsvMetricsLine.Header + "\n")
    }
    csvLines.foreach { line ⇒
      fout.write(new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").format(new Date(tick.to)) + CsvMetricsLine.Separator + line.csvLine + "\n")
    }
    fout.flush()
    fout.close()
  }

  case class CsvMetricsLine(
      name: String,
      metricType: String,
      count: Long,
      average: Option[Double] = None,
      min: Option[Double] = None,
      max: Option[Double] = None,
      percentile50: Option[Long] = None,
      percentile90: Option[Long] = None,
      percentile95: Option[Long] = None,
      percentile99: Option[Long] = None,
      percentile999: Option[Long] = None) {
    lazy val csvLine = CsvMetricsLine.lineToCsv(this)
  }

  object CsvMetricsLine {
    val Separator = ";"
    private val s = Separator
    val Header = s"Metric Name$s Metric Type$s Count$s Average$s Min$s Max$s 50th Percentile$s 90th Percentile$s 95th Percentile$s 99th Percentile$s 99.9th Percentile"

    def lineToCsv(line: CsvMetricsLine) = {
      val sb = new StringBuilder()
      sb.append(line.name)
      sb.append(s)
      sb.append(line.metricType)
      sb.append(s)
      sb.append(line.count)
      sb.append(s)
      sb.append(line.average.map(_.toLong).getOrElse(""))
      sb.append(s)
      sb.append(line.min.getOrElse(""))
      sb.append(s)
      sb.append(line.max.getOrElse(""))
      sb.append(s)
      sb.append(line.percentile50.getOrElse(""))
      sb.append(s)
      sb.append(line.percentile90.getOrElse(""))
      sb.append(s)
      sb.append(line.percentile95.getOrElse(""))
      sb.append(s)
      sb.append(line.percentile99.getOrElse(""))
      sb.append(s)
      sb.append(line.percentile999.getOrElse(""))
      sb.toString()
    }

    def apply(name: String, snapshot: Histogram.Snapshot): CsvMetricsLine =
      CsvMetricsLine(
        name = name,
        metricType = "histogram",
        count = snapshot.numberOfMeasurements,
        average = Some(snapshot.average),
        min = Some(snapshot.min),
        max = Some(snapshot.max),
        percentile50 = Some(snapshot.percentile(0.50F)),
        percentile90 = Some(snapshot.percentile(0.90F)),
        percentile95 = Some(snapshot.percentile(0.95F)),
        percentile99 = Some(snapshot.percentile(0.99F)),
        percentile999 = Some(snapshot.percentile(0.999F)))

    def apply(name: String, snapshot: Counter.Snapshot): CsvMetricsLine =
      CsvMetricsLine(
        name = name,
        metricType = "counter",
        count = snapshot.count)
  }

  def createActorMetrics(name: String, ams: ActorMetricSnapshot): List[CsvMetricsLine] = {
    List(
      CsvMetricsLine(s"${name}.processingTime", ams.processingTime),
      CsvMetricsLine(s"${name}.timeInMailbox", ams.timeInMailbox),
      CsvMetricsLine(s"${name}.mailboxSize", ams.mailboxSize),
      CsvMetricsLine(s"${name}.errors", ams.errors))
  }

  def createCpuMetrics(cms: CPUMetricSnapshot): List[CsvMetricsLine] = {
    List(
      CsvMetricsLine("cpu.user", cms.user),
      CsvMetricsLine("cpu.system", cms.system),
      CsvMetricsLine("cpu.wait", cms.cpuWait),
      CsvMetricsLine("cpu.idle", cms.idle))
  }

  def createNetworkMetrics(nms: NetworkMetricSnapshot): List[CsvMetricsLine] = {
    List(
      CsvMetricsLine("network.rxBytes", nms.rxBytes),
      CsvMetricsLine("network.txBytes", nms.txBytes),
      CsvMetricsLine("network.rxErrors", nms.rxErrors),
      CsvMetricsLine("network.rxErrors", nms.txErrors))
  }

  def createProcessCpuMetrics(pcms: ProcessCPUMetricsSnapshot): List[CsvMetricsLine] = {
    List(
      CsvMetricsLine("process.cpuPercent", pcms.cpuPercent),
      CsvMetricsLine("process.totalProcessTime", pcms.totalProcessTime))
  }

  def logTraceMetrics(name: String, tms: TraceMetricsSnapshot): Unit = {
    // TODO: check how to write to CSV
    //    val traceMetricsData = StringBuilder.newBuilder
    //
    //    traceMetricsData.append(
    //      """
    //        |+--------------------------------------------------------------------------------------------------+
    //        ||                                                                                                  |
    //        ||    Trace: %-83s    |
    //        ||    Count: %-8s                                                                               |
    //        ||                                                                                                  |
    //        ||  Elapsed Time (nanoseconds):                                                                     |
    //        |"""
    //        .stripMargin.format(
    //          name, tms.elapsedTime.numberOfMeasurements))
    //
    //    traceMetricsData.append(compactHistogramView(tms.elapsedTime))
    //    traceMetricsData.append(
    //      """
    //        ||                                                                                                  |
    //        |+--------------------------------------------------------------------------------------------------+"""
    //        .stripMargin)
    //
    //    log.info(traceMetricsData.toString())
  }
}

object CsvReporterSubscriber {

  implicit class RichHistogramSnapshot(histogram: Histogram.Snapshot) {
    def percentile(q: Float): Long = {
      val records = histogram.recordsIterator
      val qThreshold = histogram.numberOfMeasurements * q
      var countToCurrentLevel = 0L
      var qLevel = 0L

      while (countToCurrentLevel < qThreshold && records.hasNext) {
        val record = records.next()
        countToCurrentLevel += record.count
        qLevel = record.level
      }

      qLevel
    }

    def average: Double = {
      if (histogram.numberOfMeasurements == 0) {
        return 0.0
      }

      var acc = 0L
      for (record ← histogram.recordsIterator) {
        acc += record.count * record.level
      }

      return acc / histogram.numberOfMeasurements
    }

    def total: Long = {
      histogram.recordsIterator.foldLeft(0L) { (acc, record) ⇒
        {
          acc + (record.count * record.level)
        }
      }
    }
  }
}
