# frozen_string_literal: true

module Sequel
  module Extensions
    module Batches
      MissingPKError = Class.new(StandardError)
      NullPKError = Class.new(StandardError)
      InvalidPKError = Class.new(StandardError)

      def in_batches(**options, &block)
        Sequel::Extensions::Batches::Yielder.new(ds: self, **options).call(&block)
      end
    end
  end
end

::Sequel::Dataset.register_extension(:batches, Sequel::Extensions::Batches)

require_relative "batches/yielder"
