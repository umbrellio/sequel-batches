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

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  config.before(:all) do
    DB.drop_table?(:data)
    DB.create_table?(:data) do
      primary_key :id
      String   :created_at
      Integer  :value
    end

    data = YAML.load(IO.read("spec/fixtures/data.yml"))

    DB[:data].multi_insert(data)
  end

  config.after(:all) do
    DB.drop_table?(:data)
  end

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end
