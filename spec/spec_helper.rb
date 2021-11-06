# frozen_string_literal: true

def is_jruby?
  RUBY_ENGINE == "jruby"
end

require "simplecov"
require "simplecov-lcov"

SimpleCov::Formatter::LcovFormatter.config do |c|
  c.report_with_single_file = true
  c.single_report_path = "coverage/lcov.info"
end
SimpleCov.formatter = SimpleCov::Formatter::MultiFormatter.new([
  SimpleCov::Formatter::HTMLFormatter,
  SimpleCov::Formatter::LcovFormatter,
])

SimpleCov.minimum_coverage(100) unless is_jruby?
SimpleCov.start

require "bundler/setup"
require "sequel"
require "logger"
require "yaml"

DB_HOST = (ENV["PGHOST"] || "localhost").freeze
DB_NAME = (ENV["DB_NAME"] || "batches_test").freeze
DB_USER = (ENV["PGUSER"] || "").freeze

def connect
  schema = is_jruby? ? "jdbc:postgresql" : "postgres"
  Sequel.connect("#{schema}://#{DB_USER}@#{DB_HOST}/#{DB_NAME}").tap(&:tables)
rescue Sequel::DatabaseConnectionError => error
  raise unless error.message.include? "database \"#{DB_NAME}\" does not exist"

  `createdb #{DB_NAME}`
  Sequel.connect("#{schema}://#{DB_USER}@#{DB_HOST}/#{DB_NAME}")
end

DB = connect
DB.logger = Logger.new("log/db.log")

RSpec.configure do |config|
  # Enable flags like --only-failures and --next-failure
  config.example_status_persistence_file_path = ".rspec_status"

  # Disable RSpec exposing methods globally on `Module` and `main`
  config.disable_monkey_patching!

  config.before(:all) do
    DB.extension :batches

    %i[data data2].each do |table|
      DB.drop_table?(table)
      DB.create_table?(table) do
        primary_key :id
        column :created_at, "text"
        column :value, "int"
      end

      DB[table].multi_insert(YAML.load_file("./spec/fixtures/data.yml"))
    end

    DB.drop_table?(:points)
    DB.create_table?(:points) do
      column :x, "int"
      column :y, "int"
      column :z, "int"
    end

    DB[:points].multi_insert(YAML.load_file("./spec/fixtures/points.yml"))
  end

  config.around do |example|
    DB.transaction do
      example.run
      raise Sequel::Rollback
    end
  end

  config.expect_with :rspec do |c|
    c.syntax = :expect
  end
end
