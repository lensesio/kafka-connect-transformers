package com.datamountaineer.streamreactor.connect.transforms

import java.util

import com.datamountaineer.streamreactor.connect.transforms.MapKeyToString.Value
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.source.SourceRecord
import org.apache.kafka.connect.transforms.util.Requirements.requireStruct
import org.scalatest.{Matchers, WordSpec}

import scala.collection.JavaConversions._

class TestMapKeyToString extends WordSpec with Matchers {
  val MAP_SCHEMA = SchemaBuilder.map(Schema.OPTIONAL_INT64_SCHEMA, Schema.OPTIONAL_STRING_SCHEMA).optional().build();

  val FIELDS_CONFIG = "fields"

  "should transform all map key to string schema" in {
    val transform = new Value[SourceRecord];
    transform.configure(Map(
      FIELDS_CONFIG -> "map1, map2")
    )

    val transformedRecord = transform.apply(mockRecord(true));

    val value = requireStruct(transformedRecord.value, null)
    val schema = transformedRecord.valueSchema

    schema.field("map1").schema().keySchema().`type`().getName shouldBe "string"
    value.getMap("map1").get("1").toString shouldBe "value1-1"

    schema.field("map2").schema().keySchema().`type`().getName shouldBe "string"
    value.getMap("map2").get("1").toString shouldBe "value2-1"
  }

  "should transform only one map key to string schema" in {
    val transform = new Value[SourceRecord];
    transform.configure(Map(
      FIELDS_CONFIG -> "map1")
    )

    val transformedRecord = transform.apply(mockRecord(true));

    val value = requireStruct(transformedRecord.value, null)
    val schema = transformedRecord.valueSchema

    schema.field("map1").schema().keySchema().`type`().getName shouldBe "string"
    value.getMap("map1").get("1").toString shouldBe "value1-1"

    schema.field("map2").schema().keySchema().`type`().getName shouldBe "int64"
    value.getMap("map2").get(1L).toString shouldBe "value2-1"
  }

  private def mockRecord(withSchema: Boolean) = {
    val simpleStructSchema = SchemaBuilder.struct.name("name").version(1).doc("doc")
      .field("magic", Schema.OPTIONAL_INT64_SCHEMA)
      .field("map1", MAP_SCHEMA)
      .field("map2", MAP_SCHEMA)
      .build

    val simpleStruct = new Struct(simpleStructSchema)
      .put("magic", 42L)
      .put("map1", new util.HashMap[Long, String]{
        put(1L,"value1-1")
        put(2L,"value1-2")
      })
      .put("map2", new util.HashMap[Long, String]{
        put(1L,"value2-1")
        put(2L,"value2-2")
      })

    new SourceRecord(null, null, "test", 0, if (withSchema) simpleStructSchema else null, simpleStruct)
  }

}
