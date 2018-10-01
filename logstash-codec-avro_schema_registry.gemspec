Gem::Specification.new do |s|
  s.name          = 'logstash-codec-avro_schema_registry'
  s.version       = '1.1.1'
  s.licenses      = ['Apache License (2.0)']
  s.summary         = "Encode and decode avro formatted data from a Confluent schema registry"
  s.description     = "Encode and decode avro formatted data from a Confluent schema registry"
  s.authors         = ["RevPoint Media"]
  s.email           = 'tech@revpointmedia.com'
  s.homepage        = "https://github.com/revpoint/logstash-codec-avro_schema_registry"
  s.require_paths   = ["lib"]

  # Files
  s.files = Dir['lib/**/*','spec/**/*','vendor/**/*','*.gemspec','*.md','CONTRIBUTORS','Gemfile','LICENSE','NOTICE.TXT']
   # Tests
  s.test_files = s.files.grep(%r{^(test|spec|features)/})

  # Special flag to let us know this is actually a logstash plugin
  s.metadata = { "logstash_plugin" => "true", "logstash_group" => "codec" }

  # Gem dependencies
  s.add_runtime_dependency "logstash-core-plugin-api", ">= 1.60", "<= 2.99"
  s.add_runtime_dependency "logstash-codec-line"
  s.add_runtime_dependency "avro"  #(Apache 2.0 license)
  s.add_runtime_dependency "schema_registry", ">= 0.1.0" #(MIT license)
  s.add_development_dependency "logstash-devutils"
end
