# frozen_string_literal: true

require "bundler/gem_tasks"
require "bundler/audit/task"
require "rspec/core/rake_task"
require "rubocop/rake_task"

RSpec::Core::RakeTask.new(:spec)
RuboCop::RakeTask.new(:lint) do |t|
  config_path = File.expand_path(File.join(".rubocop.yml"), __dir__)

  t.options = ["--config", config_path]
  t.requires << "rubocop-rspec"
  t.requires << "rubocop-performance"
end
Bundler::Audit::Task.new

task default: %i[lint spec]
