RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end
require 'coveralls'
Coveralls.wear!
require "bundler/setup"
require "sequel/extensions/batches"
require "logger"

DB_NAME = (ENV['DB_NAME'] || "batches_test").freeze

def connect
  jruby = (defined?(RUBY_ENGINE) && RUBY_ENGINE == 'jruby') || defined?(JRUBY_VERSION)
  schema = jruby ? "jdbc:postgresql" : "postgres"
  Sequel.connect("#{schema}:///#{DB_NAME}").tap(&:tables)
rescue Sequel::DatabaseConnectionError => e
  raise unless e.message.include? "database \"#{DB_NAME}\" does not exist"
  `createdb #{DB_NAME}`
  Sequel.connect("#{schema}:///#{DB_NAME}")
end

DB = connect
DB.logger = Logger.new("log/db.log")

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  config.before(:all) do
    DB.drop_table?(:data)
    DB.create_table?(:data) do
      primary_key :id
      column :created_at, "text"
      column :value, "int"
    end

    DB[:data].multi_insert(YAML.load(IO.read("spec/fixtures/data.yml")))

    DB.drop_table?(:points)
    DB.create_table?(:points) do
      column :x, "int"
      column :y, "int"
      column :z, "int"
    end

    DB[:points].multi_insert(YAML.load(IO.read("spec/fixtures/points.yml")))
  end

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end
