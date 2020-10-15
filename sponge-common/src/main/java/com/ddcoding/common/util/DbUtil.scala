package com.ddcoding.common.util

import com.typesafe.config.Config
import scalikejdbc.{ConnectionPool, ConnectionPoolSettings}

object DbUtil {
  def init(config: Config) = {
    val driver = config.getString("driver")
    val user = config.getString("user")
    val passwd = config.getString("passwd")
    val url = config.getString("url")
    val settings = ConnectionPoolSettings(
      initialSize = config.getInt("init_size"),
      maxSize = config.getInt("max_size"),
      connectionTimeoutMillis = config.getLong("timeout"),
      validationQuery = "select 1 from dual"
    )

    Class.forName(driver)
    ConnectionPool.singleton(url, user, passwd, settings)
    ConnectionPool.add("common9", url, user, passwd, settings)
  }

  def close() = {
    ConnectionPool.closeAll()
  }
}
