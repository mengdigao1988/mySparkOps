package cn.mengdi.spark.jobs


import org.apache.spark.sql.SparkSession
import cn.mengdi.spark.util.myImplicits._


object findOldestAndYoungestGuy {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("topkByKey")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    spark.sparkContext.setLogLevel("warn")

    // create dataset with Person
    val ds = Seq(
      Person("us", "jack", 1),
      Person("cn", "gao", 30),
      Person("us", "johnson", 50),
      Person("us", "william", 60),
      Person("cn", "li", 10),
      Person("jp", "yui", 5)
    ).toDS().cache()

    // show youngest 1 guy within each nation
    ds.bottomkByKey(2, func = u => u.nation).show()

    // show oldest 1 guy within each nation
    ds.topkByKey(2, func = u => u.nation).show()


    spark.stop()

  }

  // Person that extends trait Ordered
  case class Person(nation: String, name: String, age: Int) extends Ordered[Person] {
    // compare Person with age
    override def compare(that: Person): Int = {
      if (this.age > that.age) {
        1
      } else {
        -1
      }
    }
  }
}
