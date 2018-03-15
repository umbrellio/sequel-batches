require "sequel/extensions/batches/version"
require "sequel/model"

module Sequel
  module Extensions
    module Batches
      MissingPKError = Class.new(StandardError)

      def in_batches(pk: nil, of: 1000, start: {}, finish: {})
        pk ||= self.db.schema(first_source)
                 .select{|r| r[1][:primary_key]}
                 .map(&:first) or raise MissingPKError

        pk_expr = pk.map do |col|
          Sequel.as(
            Sequel.pg_array(
              [
                Sequel.function(:min, col),
                Sequel.function(:max, col)
              ]
            ), :"#{col}"
          )
        end

        entire_min_max = self.order(*pk).select(*pk_expr).first
        min_max = {}

        range_expr =  -> (col, range) { Sequel.&(Sequel.expr(col) >= range[0], Sequel.expr(col) <= range[1]) }

        loop do
          pk.each do |col|
            entire_min_max[col][0] = start[col] || entire_min_max[col][0]
            entire_min_max[col][1] = finish[col] || entire_min_max[col][1]
          end

          ds = self.order(*pk).limit(of).where(*pk.map { |col| range_expr.call(col, entire_min_max[col]) })
          ds = ds.where(*min_max.map { |k,v| Sequel.expr(k) > v[1] }) if min_max.present?
          min_max = self.db.from(ds).select(*pk_expr).first

          break if min_max.values.flatten.any?(&:blank?)
          yield self.where(*pk.map { |col| range_expr.call(col, min_max[col]) })
        end
      end

      private

      ::Sequel::Dataset.register_extension(:batches, Batches)
    end
  end
end
