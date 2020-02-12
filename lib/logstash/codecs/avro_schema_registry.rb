# encoding: utf-8
require "avro"
require "open-uri"
require "schema_registry"
require "schema_registry/client"
require "logstash/codecs/base"
require "logstash/namespace"
require "logstash/event"
require "logstash/timestamp"
require "logstash/util"
require "base64"

MAGIC_BYTE = 0

# == Logstash Codec - Avro Schema Registry
#
# This plugin is used to serialize Logstash events as
# Avro datums, as well as deserializing Avro datums into
# Logstash events.
#
# Decode/encode Avro records as Logstash events using the
# associated Avro schema from a Confluent schema registry.
# (https://github.com/confluentinc/schema-registry)
#
#
# ==== Decoding (input)
#
# When this codec is used to decode the input, you may pass the following options:
# - ``endpoint`` - always required.
# - ``username`` - optional.
# - ``password`` - optional.
# - ``tag_on_failure`` - tag events with ``_avroparsefailure`` when decode fails
#
# If the input stream is binary encoded, you should use the ``ByteArrayDeserializer``
# in the Kafka input config.
#
# ==== Encoding (output)
#
# This codec uses the Confluent schema registry to register a schema and
# encode the data in Avro using schema_id lookups.
#
# When this codec is used to encode, you may pass the following options:
# - ``endpoint`` - always required.
# - ``username`` - optional.
# - ``password`` - optional.
# - ``schema_id`` - when provided, no other options are required.
# - ``subject_name`` - required when there is no ``schema_id``.
# - ``schema_version`` - when provided, the schema will be looked up in the registry.
# - ``schema_uri`` - when provided, JSON schema is loaded from URL or file.
# - ``schema_string`` - required when there is no ``schema_id``, ``schema_version`` or ``schema_uri``
# - ``check_compatibility`` - will check schema compatibility before encoding.
# - ``register_schema`` - will register the JSON schema if it does not exist.
# - ``binary_encoded`` - will output the encoded event as a ByteArray.
# - ``base64_encoded`` - will output in base64 encoding, deafault is true
#   Requires the ``ByteArraySerializer`` to be set in the Kafka output config.
# - ``client_certificate`` -  Client TLS certificate for mutual TLS
# - ``client_key`` -  Client TLS key for mutual TLS
# - ``ca_certificate`` -  CA Certificate
# - ``verify_mode`` -  SSL Verify modes.  Valid options are `verify_none`, `verify_peer`, `verify_client_once`,
#   and `verify_fail_if_no_peer_cert`.  Default is `verify_peer`
#
# ==== Usage
# Example usage with Kafka input and output.
#
# [source,ruby]
# ----------------------------------
# input {
#   kafka {
#     ...
#     codec => avro_schema_registry {
#       endpoint => "http://schemas.example.com"
#     }
#     value_deserializer_class => "org.apache.kafka.common.serialization.ByteArrayDeserializer"
#   }
# }
# filter {
#   ...
# }
# output {
#   kafka {
#     ...
#     codec => avro_schema_registry {
#       endpoint => "http://schemas.example.com"
#       subject_name => "my_kafka_subject_name"
#       schema_uri => "/app/my_kafka_subject.avsc"
#       register_schema => true
#     }
#     value_serializer => "org.apache.kafka.common.serialization.ByteArraySerializer"
#   }
# }
# ----------------------------------
#
# Using signed certificate for registry authentication
#
# [source,ruby]
# ----------------------------------
# output {
#   kafka {
#     ...
#     codec => avro_schema_registry {
#       endpoint => "http://schemas.example.com"
#       schema_id => 47
#       client_key          => "./client.key"
#       client_certificate  => "./client.crt"
#       ca_certificate      => "./ca.pem"
#       verify_mode         => "verify_peer"
#     }
#     value_serializer => "org.apache.kafka.common.serialization.ByteArraySerializer"
#   }
# }
# ----------------------------------

class LogStash::Codecs::AvroSchemaRegistry < LogStash::Codecs::Base
  config_name "avro_schema_registry"
  
  EXCLUDE_ALWAYS = [ "@timestamp", "@version" ]

  # schema registry endpoint and credentials
  config :endpoint, :validate => :string, :required => true
  config :username, :validate => :string, :default => nil
  config :password, :validate => :string, :default => nil

  config :schema_id, :validate => :number, :default => nil
  config :subject_name, :validate => :string, :default => nil
  config :schema_version, :validate => :number, :default => nil
  config :schema_uri, :validate => :string, :default => nil
  config :schema_string, :validate => :string, :default => nil
  config :check_compatibility, :validate => :boolean, :default => false
  config :register_schema, :validate => :boolean, :default => false
  config :binary_encoded, :validate => :boolean, :default => false
  config :base64_encoded, :validate => :boolean, :default => false

  # tag events with `_avroparsefailure` when decode fails
  config :tag_on_failure, :validate => :boolean, :default => false

  config :client_certificate, :validate => :string, :default => nil
  config :client_key, :validate => :string, :default => nil
  config :ca_certificate, :validate => :string, :default => nil
  config :verify_mode, :validate => :string, :default => 'verify_peer'
  
  public
  def register
    @client = if client_certificate != nil
      SchemaRegistry::Client.new(endpoint, username, password, SchemaRegistry::Client.connection_options(
        client_certificate: client_certificate,
        client_key: client_key,
        ca_certificate: ca_certificate,
        verify_mode: verify_mode
      ))
    else
      SchemaRegistry::Client.new(endpoint, username, password)
    end

    @schemas = Hash.new
    @write_schema_id = nil
  end

  def get_schema(schema_id)
    unless @schemas.has_key?(schema_id)
      @schemas[schema_id] = Avro::Schema.parse(@client.schema(schema_id))
    end
    @schemas[schema_id]
  end

  def load_schema_json()
    if @schema_uri
      open(@schema_uri).read
    elsif @schema_string
      @schema_string
    else
      @logger.error('you must supply a schema_uri or schema_string in the config')
    end
  end

  def get_write_schema_id()
    # If schema id is passed, just use that
    if @schema_id
      @schema_id

    else
      # subject_name is required
      if @subject_name == nil
        @logger.error('requires a subject_name')
      else
        subject = @client.subject(@subject_name)

        # If schema_version, load from subject API
        if @schema_version != nil
          schema = subject.version(@schema_version)

        # Otherwise, load schema json and check with registry
        else
          schema_json = load_schema_json

          # If not compatible, raise error
          if @check_compatibility
            unless subject.compatible?(schema_json)
              @logger.error('the schema json is not compatible with the subject. you should fix your schema or change the compatibility level.')
            end
          end

          if @register_schema
            subject.register_schema(schema_json) unless subject.schema_registered?(schema_json)
          end

          schema = subject.verify_schema(schema_json)
        end
        # Return schema id
        schema.id
      end
    end
  end

  def clean_event(event)
    event_hash = event.to_hash
    event_hash.delete_if { |key, _| EXCLUDE_ALWAYS.include? key }
    event_hash
  end

  public
  def decode(data)
    if data.length < 5
      @logger.error('message is too small to decode')
    else
      datum = StringIO.new(Base64.strict_decode64(data)) rescue StringIO.new(data)
      magic_byte, schema_id = datum.read(5).unpack("cI>")
      if magic_byte != MAGIC_BYTE
        @logger.error('message does not start with magic byte')
      else
        schema = get_schema(schema_id)
        decoder = Avro::IO::BinaryDecoder.new(datum)
        datum_reader = Avro::IO::DatumReader.new(schema)
        yield LogStash::Event.new(datum_reader.read(decoder))
      end
    end
  rescue => e
    if tag_on_failure
      @logger.error("Avro parse error, original data now in message field", :error => e)
      yield LogStash::Event.new("message" => data, "tags" => ["_avroparsefailure"])
    else
      raise e
    end
  end

  public
  def encode(event)
    @write_schema_id ||= get_write_schema_id
    schema = get_schema(@write_schema_id)
    dw = Avro::IO::DatumWriter.new(schema)
    buffer = StringIO.new
    buffer.write(MAGIC_BYTE.chr)
    buffer.write([@write_schema_id].pack("I>"))
    encoder = Avro::IO::BinaryEncoder.new(buffer)
    dw.write(clean_event(event), encoder)
    if @binary_encoded
       @on_event.call(event, buffer.string.to_java_bytes)
    elsif @base64_encoded
       @on_event.call(event, Base64.strict_encode64(buffer.string))
    else
       @on_event.call(event, buffer.string)
    end
  end
end
