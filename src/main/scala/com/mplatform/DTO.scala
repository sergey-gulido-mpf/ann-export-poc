package com.mplatform


class DTO {}

case class Dto(addReduced: Array[String],
               delReduced: Array[String],
               visitorMappings: Array[VisitorMappings],
               accId: Int,
               dspId: Int,
               seatId: Int) {

  def toMapping: Seq[Mapping] = {
    visitorMappings.map(vMap => Mapping(addReduced, delReduced, vMap.id, accId, dspId, seatId, vMap.deviceType.getOrElse("0")))
  }
}

case class VisitorMappings(id: String, exportType: String, deviceType: Option[String])

case class Mapping(addReduced: Array[String],
                   delReduced: Array[String],
                   visitorId: String,
                   accId: Int,
                   dspId: Int,
                   seatId: Int,
                   deviceType: String) {
}

case class GroupedMapping(visitorId: String,
                          accId: Int,
                          seatId: Int,
                          deviceType: String,
                          adds: Array[Array[String]],
                          dels: Array[Array[String]]) {

  def toAdobeFormat: AdobeFormat = {
    val stringBuilder = new StringBuilder(visitorId)
    stringBuilder ++= " "
    adds.flatten
      .toSet
      .foldLeft(stringBuilder)((b, s) => b ++= "d_sid=" ++= s ++= ",")
    dels.flatten
      .toSet
      .foldLeft(stringBuilder)((b, s) => b ++= "d_unsid=" ++= s ++= ",")
    stringBuilder.deleteCharAt(stringBuilder.length - 1)
    AdobeFormat(accId, seatId, deviceType, stringBuilder.toString())
  }
}

case class AdobeFormat(accId: Int, seatId: Int, deviceType: String, result: String)