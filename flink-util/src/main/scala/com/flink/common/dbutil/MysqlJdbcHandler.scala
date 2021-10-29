/*
* Author:liuwenzhong@kuaishou.com
* Date:2021-09-21
*/
package com.flink.common.dbutil

import java.sql.{Connection, DriverManager}

object MysqlJdbcHandler {
  var conn: Connection = null
  /*
   *
   */
  def getMysqlConn(jdbc: String, user: String, pass: String): Connection = {
    // scalastyle:off
    Class.forName("com.mysql.jdbc.Driver")
    // scalastyle:on
    DriverManager.getConnection(jdbc, user, pass)
  }

  /**
   * 获取mysq的全局连接
   * @param jdbc
   * @param user
   * @param pass
   * @return
   */
  def getMysqlGlobalConn(jdbc: String,
                         user: String,
                         pass: String): Connection = {
    initMysql(jdbc, user, pass)
    conn
  }

  /**
   * 初始化mysql连接
   * @param jdbc
   * @param user
   * @param pass
   */
  def initMysql(jdbc: String, user: String, pass: String) {
    if (null == conn || conn.isClosed) {
      // scalastyle:off
      Class.forName("com.mysql.jdbc.Driver")
      // scalastyle:on
      conn = DriverManager.getConnection(jdbc, user, pass)
    }
  }

  def deleteData(jdbc: String, user: String, pass: String, sql: String): Unit = {
    try {
      val con = getMysqlConn(jdbc, user, pass)
      val stmt = con .createStatement()
      stmt.executeUpdate(sql)
      stmt.close()
      con.close()
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
  }

  def main(args: Array[String]): Unit = {
    var url = "admin.e.corp.kuaishou.com"
    //val jdbc = "jdbc:mysql://172.29.36.175:3306/db_admin"
    val jdbc = "jdbc:mysql://10.62.225.50:3306/db_admin"
    val user = "root"
    val passw = "3attZYgWK8QKF!w"
    // val passw = "123456"
    val conn = getMysqlConn(jdbc, user, passw)
    val sql = "select * from tb_admin_user"
    // val sql = s"""replace into $tb(id,aa,bb) values(?,?,?)"""
    //    val sql =
    //      s"""insert into $tb(id,aa,bb) values(?,?,?)
    //         | ON DUPLICATE KEY UPDATE
    //         |  id =VALUES(id), aa = VALUES(aa) , bb = VALUES(bb)""".stripMargin
    conn.close()

  }

}
