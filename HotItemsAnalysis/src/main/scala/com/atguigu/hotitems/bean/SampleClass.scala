package com.atguigu.hotitems.bean

/**
  * 用户行为日志样例类
  *
  * @param userId: 用户id
  * @param itemId: 商品id
  * @param categoryId: 商品品类id
  * @param behavior: 事件类型
  * @param timestamp: 时间戳
  */
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

/**
  * 商品浏览量样例类
  *
  * @param itemId: 商品id
  * @param windowEnd: 统计窗口结束时间
  * @param count: 访问次数
  */
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

