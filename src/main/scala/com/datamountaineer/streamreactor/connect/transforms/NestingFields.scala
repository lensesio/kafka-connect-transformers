package com.datamountaineer.streamreactor.connect.transforms

import java.util
import java.util.List

import org.apache.kafka.common.cache.{Cache, LRUCache, SynchronizedCache}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data.{Schema, SchemaBuilder, Struct}
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements.{requireMap, requireStruct}
import org.apache.kafka.connect.transforms.util.{SchemaUtil, SimpleConfig}

import scala.collection.JavaConversions._

object NestingFields {
  private val PURPOSE = "nesting fields from value to new field"
  private val NESTED_NAME_CONFIG = "nested.name"
  private val FIELDS_CONFIG = "fields"

  private val CONFIG_DEF: ConfigDef = new ConfigDef()
    .define(NESTED_NAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "Nested field name.")
    .define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Field names to add in the nested field.")

  class Key[R <: ConnectRecord[R]] extends NestingFields[R] {
    override protected def operatingSchema(record: R): Schema = record.keySchema

    override protected def operatingValue(record: R): Any = record.key

    override protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Any): R =
      record.newRecord(record.topic, record.kafkaPartition, updatedSchema, updatedValue, record.valueSchema, record.value, record.timestamp)
  }

  class Value[R <: ConnectRecord[R]] extends NestingFields[R] {
    override protected def operatingSchema(record: R): Schema = record.valueSchema

    override protected def operatingValue(record: R): Any = record.value

    override protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Any): R =
      record.newRecord(record.topic, record.kafkaPartition, record.keySchema, record.key, updatedSchema, updatedValue, record.timestamp)
  }

}

abstract class NestingFields[R <: ConnectRecord[R]] extends Transformation[R] {
  private var fields: List[String] = _
  private var nestedName: String = _
  private var schemaUpdateCache: Cache[Schema, Schema] = _

  override def configure(props: util.Map[String, _]): Unit = {
    val config = new SimpleConfig(NestingFields.CONFIG_DEF, props)
    nestedName = config.getString(NestingFields.NESTED_NAME_CONFIG)
    fields = config.getList(NestingFields.FIELDS_CONFIG)
    schemaUpdateCache = new SynchronizedCache[Schema, Schema](new LRUCache[Schema, Schema](16))
  }

  override def apply(record: R): R =
    if (operatingSchema(record) == null) applySchemaless(record) else applyWithSchema(record)

  private def applySchemaless(record: R) = {
    val value = requireMap(operatingValue(record), NestingFields.PURPOSE)
    val updatedValue = new util.HashMap[String, AnyRef](value)

    updatedValue.put(nestedName, value.filterKeys(k => fields.contains(k)))
    newRecord(record, null, updatedValue)
  }

  private def applyWithSchema(record: R) = {
    val value = requireStruct(operatingValue(record), NestingFields.PURPOSE)
    var updatedSchema = schemaUpdateCache.get(value.schema)
    if (updatedSchema == null) {
      updatedSchema = makeUpdatedSchema(value.schema)
      schemaUpdateCache.put(value.schema, updatedSchema)
    }
    val newNestedSchema = updatedSchema.field(nestedName).schema
    val newNestedValue = new Struct(newNestedSchema)
    val updatedValue = new Struct(updatedSchema)
    
    for (field <- value.schema.fields) {
      updatedValue.put(field.name, value.get(field))
    }
    
    for (field <- value.schema.fields) {
      if (fields.contains(field.name)) newNestedValue.put(field.name, value.get(field))
    }

    updatedValue.put(nestedName, newNestedValue)
    newRecord(record, updatedSchema, updatedValue)
  }

  private def makeUpdatedSchema(schema: Schema) = {
    val builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct)
    val nestedStruct = SchemaBuilder.struct
    builder.field(nestedName, nestedStruct)
    import scala.collection.JavaConversions._
    for (field <- schema.fields) {
      builder.field(field.name, field.schema)
      if (fields.contains(field.name)) nestedStruct.field(field.name, field.schema)
    }
    builder.build
  }

  override def close(): Unit = {
    schemaUpdateCache = null
  }

  override def config: ConfigDef = NestingFields.CONFIG_DEF

  protected def operatingSchema(record: R): Schema

  protected def operatingValue(record: R): Any

  protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Any): R
}
