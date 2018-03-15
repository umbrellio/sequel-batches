require "sequel/extensions/batches/version"
require "sequel/model"

module Sequel
  module Extensions
    module Batches
      MissingPKError = Class.new(StandardError)
      def in_batches(pk: nil, of: 1000, start: nil, finish: nil)
        pk = self.db.schema(first_source).select{|r| r.second[:primary_key]}.map(&:first) or raise MissingPKError
        pk_expr = pk.map { |col| Sequel.as(Sequel.pg_array([Sequel.function(:min, col), Sequel.function(:max, col)]), :"#{col}") }

        loop do
          min_max = DB.from(self.order(*pk).limit(of)).select(*pk_expr).first

          break if min_max.values.flatten.any?(&:blank?)
          yield self.where(pk.map { |col| [col, Range.new(*min_max[col])]})
        end
      end

      private

      ::Sequel::Dataset.register_extension(:batches, Batches)
    end
  end
end
