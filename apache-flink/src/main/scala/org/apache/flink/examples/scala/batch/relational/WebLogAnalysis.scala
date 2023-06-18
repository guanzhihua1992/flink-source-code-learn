package org.apache.flink.examples.scala.batch.relational

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala._
import org.apache.flink.examples.java.batch.relational.util.WebLogData
import org.apache.flink.util.Collector

/**
 * 日志分析样例
 * 实现的是以下sql
 * {{{
 * SELECT
 *       r.pageURL,
 *       r.pageRank,
 *       r.avgDuration
 * FROM documents d JOIN rankings r
 *                  ON d.url = r.url
 * WHERE CONTAINS(d.text, [keywords])
 *       AND r.rank > [rank]
 *       AND NOT EXISTS
 *           (
 *              SELECT * FROM Visits v
 *              WHERE v.destUrl = d.url
 *                    AND v.visitDate < [date]
 *           );
 *
 * 先看子查询    SELECT * FROM Visits v WHERE v.destUrl = d.url AND v.visitDate < [date]
 * 访问记录中访问时间小于date
 *
 * 所以整个sql目标是  documents 关联 rankings 后
 * * d.text 内容包含 keywords r.rank 大于 rank 且 访问日期在date之后的 rankings 表中数据
 *
 * }}}
 * 三张表建表语句如下
 * {{{
 * Documents url及文本
 * CREATE TABLE Documents (
 *                url VARCHAR(100) PRIMARY KEY,
 *                contents TEXT );
 *Rankings URL 排名 平均访问时长
 * CREATE TABLE Rankings (
 *                pageRank INT,
 *                pageURL VARCHAR(100) PRIMARY KEY,
 *                avgDuration INT );
 *URL访问记录
 * CREATE TABLE Visits (
 *                sourceIP VARCHAR(16),
 *                destURL VARCHAR(100),
 *                visitDate DATE,
 *                adRevenue FLOAT,
 *                userAgent VARCHAR(64),
 *                countryCode VARCHAR(3),
 *                languageCode VARCHAR(6),
 *                searchWord VARCHAR(32),
 *                duration INT );
 * }}}
 */
object WebLogAnalysis {
  def main(args: Array[String]): Unit = {
    val params: ParameterTool = ParameterTool.fromArgs(args)

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment

    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    val documents = getDocumentsDataSet(env, params)
    val ranks = getRanksDataSet(env, params)
    val visits = getVisitsDataSet(env, params)

    //Documents 中 contents 字段包含 editors oscillations
    val filteredDocs = documents
      .filter(doc => doc._2.contains(" editors ") && doc._2.contains(" oscillations "))

    //Rankings 表中排名在前四十
    val filteredRanks = ranks
      .filter(rank => rank._1 > 40)

    //Visits 中访问日期是2007年
    val filteredVisits = visits
      .filter(visit => visit._2.substring(0, 4).toInt == 2007)

    //过滤后Documents关联Rankings表
    // 关联字段为 Documents第一个字段 Rankings第二个字段
    // 输出关联 后面表数据所有数据
    val joinDocsRanks = filteredDocs
      .join(filteredRanks)
      .where(0)
      .equalTo(1)((doc, rank) => rank)
      .withForwardedFieldsSecond("*")

    //上一步过滤后 Rankings 关联 Visits
    //关联字段为 Rankings 第二个字段 Visits 第一个字段
    // 输出关联 前面表数据所有数据
    //如果 visits 为空 直接输出 ranks ？
    val result = joinDocsRanks
      .coGroup(filteredVisits)
      .where(1)
      .equalTo(0) {
        (
          ranks: Iterator[(Int, String, Int)],
          visits: Iterator[(String, String)],
          out: Collector[(Int, String, Int)]) =>
          if (visits.isEmpty) for (rank <- ranks) out.collect(rank)
      }
      .withForwardedFieldsFirst("*")

    // emit result
    if (params.has("output")) {
      result.writeAsCsv(params.get("output"), "\n", "|")
      env.execute("Scala WebLogAnalysis Example")
    } else {
      println("Printing result to stdout. Use --output to specify output path.")
      result.print()
    }
  }

  private def getDocumentsDataSet(
    env: ExecutionEnvironment,
    params: ParameterTool): DataSet[(String, String)] = {
    if (params.has("documents")) {
      env.readCsvFile[(String, String)](
        params.get("documents"),
        fieldDelimiter = "|",
        includedFields = Array(0, 1))
    } else {
      println("Executing WebLogAnalysis example with default documents data set.")
      println("Use --documents to specify file input.")
      val documents = WebLogData.DOCUMENTS.map {
        case Array(x, y) => (x.asInstanceOf[String], y.asInstanceOf[String])
      }
      env.fromCollection(documents)
    }
  }

  private def getRanksDataSet(
    env: ExecutionEnvironment,
    params: ParameterTool): DataSet[(Int, String, Int)] = {
    if (params.has("ranks")) {
      env.readCsvFile[(Int, String, Int)](
        params.get("ranks"),
        fieldDelimiter = "|",
        includedFields = Array(0, 1, 2))
    } else {
      println("Executing WebLogAnalysis example with default ranks data set.")
      println("Use --ranks to specify file input.")
      val ranks = WebLogData.RANKS.map {
        case Array(x, y, z) => (x.asInstanceOf[Int], y.asInstanceOf[String], z.asInstanceOf[Int])
      }
      env.fromCollection(ranks)
    }
  }

  private def getVisitsDataSet(
    env: ExecutionEnvironment,
    params: ParameterTool): DataSet[(String, String)] = {
    if (params.has("visits")) {
      env.readCsvFile[(String, String)](
        params.get("visits"),
        fieldDelimiter = "|",
        includedFields = Array(1, 2))
    } else {
      println("Executing WebLogAnalysis example with default visits data set.")
      println("Use --visits to specify file input.")
      val visits = WebLogData.VISITS.map {
        case Array(x, y) => (x.asInstanceOf[String], y.asInstanceOf[String])
      }
      env.fromCollection(visits)
    }
  }
}
