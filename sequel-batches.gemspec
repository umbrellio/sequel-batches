# frozen_string_literal: true

lib = File.expand_path("lib", __dir__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)

Gem::Specification.new do |spec|
  spec.name = "sequel-batches"
  spec.version = "0.2.1"
  spec.authors = ["fiscal-cliff", "umbrellio"]
  spec.email = ["oss@umbrellio.biz"]

  spec.summary = "The extension mimics AR5 batches api"
  spec.description = "Allows you to split your dataset in batches"
  spec.homepage = "https://github.com/umbrellio/sequel-batches"
  spec.license = "MIT"

  spec.files = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.require_paths = ["lib"]

  spec.add_runtime_dependency "sequel"

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "coveralls"
  spec.add_development_dependency "pry"
  spec.add_development_dependency "rake"
  spec.add_development_dependency "rspec"
  spec.add_development_dependency "rubocop-config-umbrellio"
  spec.add_development_dependency "simplecov"
end
