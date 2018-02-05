package com.datamountaineer.streamreactor.connect.transforms

import java.util
import java.util.List

import org.apache.kafka.common.cache.{Cache, LRUCache, SynchronizedCache}
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.connect.connector.ConnectRecord
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.transforms.Transformation
import org.apache.kafka.connect.transforms.util.Requirements.requireStruct
import org.apache.kafka.connect.transforms.util.{SchemaUtil, SimpleConfig}

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._


object MapKeyToString {
  private val PURPOSE = "Change map key to string"
  private val FIELDS_CONFIG = "fields"

  val CONFIG_DEF: ConfigDef = new ConfigDef()
    .define(FIELDS_CONFIG, ConfigDef.Type.LIST, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH, "Field names to be cast.")

  class Key[R <: ConnectRecord[R]] extends MapKeyToString[R] {
    override protected def operatingSchema(record: R): Schema = record.keySchema

    override protected def operatingValue(record: R): Any = record.key

    override protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Any): R = record.newRecord(record.topic, record.kafkaPartition, updatedSchema, updatedValue, record.valueSchema, record.value, record.timestamp)
  }

  class Value[R <: ConnectRecord[R]] extends MapKeyToString[R] {
    override protected def operatingSchema(record: R): Schema = record.valueSchema

    override protected def operatingValue(record: R): Any = record.value

    override protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Any): R = record.newRecord(record.topic, record.kafkaPartition, record.keySchema, record.key, updatedSchema, updatedValue, record.timestamp)
  }

}

abstract class MapKeyToString[R <: ConnectRecord[R]] extends Transformation[R] {
  private var fields: List[String] = _
  private var schemaUpdateCache: Cache[Schema, Schema] = _

  override def configure(props: util.Map[String, _]): Unit = {
    val config = new SimpleConfig(MapKeyToString.CONFIG_DEF, props)
    fields = config.getList(MapKeyToString.FIELDS_CONFIG)
    schemaUpdateCache = new SynchronizedCache[Schema, Schema](new LRUCache[Schema, Schema](16))
  }

  override def apply(record: R): R =
    if (operatingSchema(record) == null) record else applyWithSchema(record)

  private def applyWithSchema(record: R) = {
    val value = requireStruct(operatingValue(record), MapKeyToString.PURPOSE)
    var updatedSchema = schemaUpdateCache.get(value.schema)
    if (updatedSchema == null) {
      updatedSchema = makeUpdatedSchema(value.schema)
      schemaUpdateCache.put(value.schema, updatedSchema)
    }
    val updatedValue = new Struct(updatedSchema)
    
    for (field <- value.schema.fields) {
      if (isChangeable(field) && value.get(field) != null) {
        val fieldValue = value.get(field).asInstanceOf[util.Map[Object, Object]];
        updatedValue.put(field.name, fieldValue.map {pair => (pair._1.toString, pair._2)}.asJava)
      } else {
        updatedValue.put(field.name, value.get(field))
      }
    }
    newRecord(record, updatedSchema, updatedValue)
  }

  private def makeUpdatedSchema(schema: Schema) = {
    val builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct)

    for (field <- schema.fields) {
      if (isChangeable(field)){
        builder.field(field.name, SchemaBuilder.map(
          Schema.OPTIONAL_STRING_SCHEMA,
          field.schema().valueSchema()
        ).optional().build())
      } else {
          builder.field(field.name, field.schema)
      }
    }
    builder.build
  }

  private def isChangeable(field: Field) = fields.contains(field.name) && field.schema.`type`() == Schema.Type.MAP

  override def close(): Unit = {
    schemaUpdateCache = null
  }

  override def config: ConfigDef = MapKeyToString.CONFIG_DEF

  protected def operatingSchema(record: R): Schema

  protected def operatingValue(record: R): Any

  protected def newRecord(record: R, updatedSchema: Schema, updatedValue: Any): R

}
