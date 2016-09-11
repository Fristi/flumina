package flumina.core.v090

import flumina.core._
import scodec.bits.ByteVector
import scodec.codecs._
import scodec.{Attempt, Codec, Err}
import scala.language.postfixOps

final case class Message(magicByte: Int, attributes: Int, key: ByteVector, value: ByteVector)

object Message {

  def computeCrc(b: ByteVector) = {
    val comp = new java.util.zip.CRC32()
    b.foldLeftBB(())((_, bb) => comp.update(bb))
    (comp.getValue & 0xffffffffL).toInt
  }

  def crcChecksum[A](codec: Codec[A]): Codec[A] = {
    def decode(crc32: Int, data: ByteVector): Attempt[A] = {
      val computedCrc32 = computeCrc(data)
      if (computedCrc32 != crc32) Attempt.failure(Err("CRC of message does not match"))
      else codec.decodeValue(data.bits)
    }

    def encode(a: A): Attempt[(Int, ByteVector)] = {
      codec.encode(a).map { bv =>
        val crc32 = computeCrc(bv.toByteVector) //TODO: conversion to array?
        crc32 -> bv.bytes
      }
    }

    (("crc32" | int32) ~ ("data" | bytes)).exmap(decode _ tupled, encode)

  }

  implicit val codec: Codec[Message] =
    crcChecksum(("magicByte" | int8) :: ("attributes" | int8) :: ("key" | kafkaBytes) :: ("value" | kafkaBytes)).as[Message]
}

final case class MessageSetEntry(offset: Long, message: Message)

object MessageSetEntry {
  implicit def messageSet(implicit message: Codec[Message]): Codec[MessageSetEntry] =
    (("offset" | int64) :: ("message" | variableSizeBytes(int32, message))).as[MessageSetEntry]
}