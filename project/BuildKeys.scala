import sbt._

object BuildKeys {
  val sparkVersion: SettingKey[String] = settingKey[String]("Spark version")
}