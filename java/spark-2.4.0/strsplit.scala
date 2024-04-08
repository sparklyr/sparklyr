package sparklyr

object StrSplit {
  def impl = (str: String, regex: String, limit: Int) => {
    if (null == str || null == regex)
      null
    else
      str.split(regex, limit)
  }
}
