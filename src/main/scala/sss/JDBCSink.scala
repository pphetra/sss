package sss


class JDBCSink(url: String, user: String, pwd: String) extends org.apache.spark.sql.ForeachWriter[org.apache.spark.sql.Row] {
  val driver = "org.postgresql.Driver"
  var connection: java.sql.Connection = _
  var statement: java.sql.Statement = _

  def open(partitionId: Long, version: Long): Boolean = {
    Class.forName(driver)
    connection = java.sql.DriverManager.getConnection(url, user, pwd)
    statement = connection.createStatement
    true
  }

  def process(value: org.apache.spark.sql.Row): Unit = {
    statement.executeUpdate("INSERT INTO public.test(subdistrict, flu, ili, sari, pneumonia, ipd, opd) " +
      "VALUES ('" + value(0) + "',"
      + value(1) + ","
      + value(2) + ","
      + value(3) + ","
      + value(4) + ","
      + value(5) + ","
      + value(6) + ")"
      + "on conflict (subdistrict) do "
      + "update "
      + "set flu = EXCLUDED.flu"
      + " ,ili = EXCLUDED.ili"
      + " ,sari = EXCLUDED.sari"
      + " ,pneumonia = EXCLUDED.pneumonia"
      + " ,ipd = EXCLUDED.ipd"
      + " ,opd = EXCLUDED.opd"
      + " ;")
  }

  def close(errorOrNull: Throwable): Unit = {
    connection.close
  }
}