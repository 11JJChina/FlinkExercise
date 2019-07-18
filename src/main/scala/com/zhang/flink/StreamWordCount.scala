package com.zhang.flink

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time


object StreamWordCount {

  case class WordWithCount(word: String, count: Long)

  def main(args: Array[String]): Unit = {

    var hostname = "localhost"
    var port = 0

    try {
      val params = ParameterTool.fromArgs(args)
      hostname = if (params.has("hostname")) params.get("hostname") else "localhost"
    } catch {
      case e: Exception => {
        System.err.println("No port specified. Please run 'SocketWindowWordCount " +
          "--hostname <hostname> --port <port>', where hostname (localhost by default) and port " +
          "is the address of the text server")
        System.err.println("To start a simple text server, run 'netcat -l <port>' " +
          "and type the input text into the command line")
        return
      }
    }

    //todo:初始化环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    //todo:加载数据
    val text = env.socketTextStream(hostname, port, '\n')

    val windowCounts = text
      .flatMap { w => w.split("\\s") }
      .map { w => WordWithCount(w, 1) }
      .keyBy("word")
      .timeWindow(Time.seconds(5))
      .sum("count")

    windowCounts.print().setParallelism(1)

    env.execute("Socket Window WordCount")

  }


}
