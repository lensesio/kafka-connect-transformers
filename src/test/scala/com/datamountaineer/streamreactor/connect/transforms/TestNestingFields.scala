package com.datamountaineer.streamreactor.connect.transforms

import java.util.Date

import com.datamountaineer.streamreactor.connect.transforms.NestingFields.Value
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.transforms.util.Requirements.requireStruct
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

class TestNestingFields extends WordSpec with Matchers {
  val OPTIONAL_TIMESTAMP_SCHEMA = Timestamp.builder().optional().build()
  val OPTIONAL_DECIMAL_SCHEMA = Decimal.builder(18).optional().build()

  private val NESTED_NAME_CONFIG = "nested.name"
  private val FIELDS_CONFIG = "fields"

  "should append another field with two nested fields when have schema" in {
    val transform = new Value[SourceRecord];
    transform.configure(Map(
      NESTED_NAME_CONFIG -> "id",
      FIELDS_CONFIG -> "dateValue1, decimalValue1")
    )

    val transformedRecord = transform.apply(mockRecord(true));

    val value = requireStruct(transformedRecord.value, null)
    val schema = transformedRecord.valueSchema

    val nestedSchema = schema.field("id").schema()
    val nestedValue =  requireStruct(value.get("id"), null)

    nestedSchema.field("dateValue1").schema().`type`() shouldBe schema.field("dateValue1").schema().`type`()
    nestedValue.get("dateValue1") shouldBe value.get("dateValue1")

    nestedSchema.field("decimalValue1").schema().`type`() shouldBe schema.field("decimalValue1").schema().`type`()
    nestedValue.get("decimalValue1") shouldBe value.get("decimalValue1")
  }

  "should append another field with one nested fields when have schema" in {
    val transform = new Value[SourceRecord];
    transform.configure(Map(
      NESTED_NAME_CONFIG -> "id",
      FIELDS_CONFIG -> "decimalValue1")
    )

    val transformedRecord = transform.apply(mockRecord(true));

    val value = requireStruct(transformedRecord.value, null)
    val schema = transformedRecord.valueSchema

    val nestedSchema = schema.field("id").schema()
    val nestedValue =  requireStruct(value.get("id"), null)

    nestedSchema.field("decimalValue1").schema().`type`() shouldBe schema.field("decimalValue1").schema().`type`()
    nestedValue.get("decimalValue1") shouldBe value.get("decimalValue1")
  }

  "should append another field with one nested fields when don't have schema" in {
    val transform = new Value[SourceRecord];
    transform.configure(Map(
      NESTED_NAME_CONFIG -> "id",
      FIELDS_CONFIG -> "decimalValue1")
    )

    val transformedRecord = transform.apply(mockRecord(true));

    val value = requireStruct(transformedRecord.value, null)

    val nestedValue =  requireStruct(value.get("id"), null)
    nestedValue.get("decimalValue1") shouldBe value.get("decimalValue1")
  }

  private def mockRecord(withSchema: Boolean) = {
    val simpleStructSchema = SchemaBuilder.struct.name("name").version(1).doc("doc")
      .field("magic", Schema.OPTIONAL_INT64_SCHEMA)
      .field("dateValue1", OPTIONAL_TIMESTAMP_SCHEMA)
      .field("decimalValue1", OPTIONAL_DECIMAL_SCHEMA)
      .build

    val simpleStruct = new Struct(simpleStructSchema)
      .put("magic", 42L)
      .put("dateValue1", new Date())
      .put("decimalValue1", BigDecimal(10.6).bigDecimal.setScale(18))

    new SourceRecord(null, null, "test", 0, if (withSchema) simpleStructSchema else null, simpleStruct)
  }

}