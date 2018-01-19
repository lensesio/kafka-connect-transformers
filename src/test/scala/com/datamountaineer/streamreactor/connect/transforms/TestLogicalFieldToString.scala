package com.datamountaineer.streamreactor.connect.transforms

import java.text.{DecimalFormat, SimpleDateFormat}
import java.util.Date

import com.datamountaineer.streamreactor.connect.transforms.LogicalFieldToString.Value
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.transforms.util.Requirements.requireStruct
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

class TestLogicalFieldToString extends WordSpec with Matchers {
  val OPTIONAL_TIMESTAMP_SCHEMA = Timestamp.builder().optional().build()
  val OPTIONAL_DECIMAL_SCHEMA = Decimal.builder(18).optional().build()

  val FORMAT_CONFIG = "format"
  val FIELDS_CONFIG = "fields"
  val DATE_FORMAT = "yyyy-MM-dd HH:mm"
  val DATE_FORMATTER = new SimpleDateFormat(DATE_FORMAT)

  val DECIMAL_FORMAT = "#,###.00"
  val DECIMAL_FORMATTER = new DecimalFormat(DECIMAL_FORMAT)

  "should transform all decimal to string schema" in {
    val transform = new Value[SourceRecord];
    transform.configure(Map(
      FORMAT_CONFIG -> DECIMAL_FORMAT,
      FIELDS_CONFIG -> "decimalValue1, decimalValue2")
    )

    val transformedRecord = transform.apply(mockRecord(true));

    val value = requireStruct(transformedRecord.value, null)
    val schema = transformedRecord.valueSchema

    schema.field("decimalValue1").schema().`type`().getName shouldBe "string"
    value.get("decimalValue1") shouldBe DECIMAL_FORMATTER.format(BigDecimal(10.6))

    schema.field("decimalValue2").schema().`type`().getName shouldBe "string"
    value.get("decimalValue2") shouldBe DECIMAL_FORMATTER.format(BigDecimal(20.7))
  }

  "should transform only one decimal to string schema" in {
    val transform = new Value[SourceRecord];
    transform.configure(Map(
      FORMAT_CONFIG -> DECIMAL_FORMAT,
      FIELDS_CONFIG -> "decimalValue1")
    )

    val transformedRecord = transform.apply(mockRecord(true));

    val value = requireStruct(transformedRecord.value, null)
    val schema = transformedRecord.valueSchema

    schema.field("decimalValue1").schema().`type`().getName shouldBe "string"
    value.get("decimalValue1") shouldBe DECIMAL_FORMATTER.format(BigDecimal(10.6))

    schema.field("decimalValue2").schema().`type`().getName shouldBe "bytes"
  }


  "should transform all date to string schema" in {
    val transform = new Value[SourceRecord];
    transform.configure(Map(
      FORMAT_CONFIG -> DATE_FORMAT,
      FIELDS_CONFIG -> "dateValue1, dateValue2")
    )

    val transformedRecord = transform.apply(mockRecord(true));

    val value = requireStruct(transformedRecord.value, null)
    val schema = transformedRecord.valueSchema

    schema.field("dateValue1").schema().`type`().getName shouldBe "string"
    value.get("dateValue1") shouldBe DATE_FORMATTER.format(new Date())

    schema.field("dateValue2").schema().`type`().getName shouldBe "string"
    value.get("dateValue2") shouldBe DATE_FORMATTER.format(new Date())
  }

  "should transform only one date to string schema" in {
    val transform = new Value[SourceRecord];
    transform.configure(Map(
      FORMAT_CONFIG -> DATE_FORMAT,
      FIELDS_CONFIG -> "dateValue1")
    )

    val transformedRecord = transform.apply(mockRecord(true));

    val value = requireStruct(transformedRecord.value, null)
    val schema = transformedRecord.valueSchema

    schema.field("dateValue1").schema().`type`().getName shouldBe "string"
    value.get("dateValue1") shouldBe DATE_FORMATTER.format(new Date())

    schema.field("dateValue2").schema().`type`().getName shouldBe "int64"
  }

  "should transform none because don't have schema" in {
    val transform = new Value[SourceRecord];
    transform.configure(Map(
      FORMAT_CONFIG -> DATE_FORMAT,
      FIELDS_CONFIG -> "decimalValue1")
    )

    val transformedRecord = transform.apply(mockRecord(false));

    val value = requireStruct(transformedRecord.value, null)

    value.get("decimalValue1") shouldBe BigDecimal(10.6).bigDecimal.setScale(4)
  }

  private def mockRecord(withSchema: Boolean) = {
    val simpleStructSchema = SchemaBuilder.struct.name("name").version(1).doc("doc")
      .field("magic", Schema.OPTIONAL_INT64_SCHEMA)
      .field("dateValue1", OPTIONAL_TIMESTAMP_SCHEMA)
      .field("dateValue2", OPTIONAL_TIMESTAMP_SCHEMA)
      .field("decimalValue1", OPTIONAL_DECIMAL_SCHEMA)
      .field("decimalValue2", OPTIONAL_DECIMAL_SCHEMA)
      .build

    val simpleStruct = new Struct(simpleStructSchema)
      .put("magic", 42L)
      .put("dateValue1", new Date())
      .put("dateValue2", new Date())
      .put("decimalValue1", BigDecimal(10.6).bigDecimal.setScale(4))
      .put("decimalValue2", BigDecimal(20.7).bigDecimal.setScale(4))

    new SourceRecord(null, null, "test", 0, if (withSchema) simpleStructSchema else null, simpleStruct)
  }

}
