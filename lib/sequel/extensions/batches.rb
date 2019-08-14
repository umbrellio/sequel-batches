require "sequel/extensions/batches/version"
require "sequel/model"

module Sequel
  module Extensions
    module Batches
      MissingPKError = Class.new(StandardError)

      def in_batches(pk: nil, of: 1000, start: nil, finish: nil)
        pk ||=
          db.schema(first_source)
            .select { |x| x[1][:primary_key] }
            .map(&:first) or raise MissingPKError

        qualified_pk = pk.map { |x| Sequel[first_source][x] }
        pk_combinations = pk.map.with_index { |x, i| pk[0..i] }

        # For composite PK (x, y, z) this will generate the following WHERE expression:
        # (x > ?) OR (x = ? AND y > ?) OR (x = ? AND y = ? AND z > ?)
        conditions = lambda do |values, mode: :start, including: true|
          or_conditions = pk_combinations.map do |keys|
            and_conditions = keys.map.with_index do |key, index|
              value = values.fetch(key)

              # All conditions should use equality except for the last one
              if index == keys.size - 1
                mode == :finish ? Sequel[key] < value : Sequel[key] > value
              else
                Sequel[key] =~ value
              end
            end

            and_conditions.reduce(:&)
          end

          if including
            including_expr = pk.map { |key| Sequel[key] =~ values.fetch(key) }.reduce(:&)
            or_conditions << including_expr
          end

          or_conditions.reduce(:|)
        end

        base_ds = order(*qualified_pk)
        base_ds = base_ds.where(conditions.call(start)) if start
        base_ds = base_ds.where(conditions.call(finish, mode: :finish)) if finish

        pk_ds = db.from(base_ds).select(*pk).order(*pk)
        actual_start = pk_ds.first
        actual_finish = pk_ds.last

        return unless actual_start && actual_finish

        base_ds = base_ds.where(conditions.call(actual_start))
        base_ds = base_ds.where(conditions.call(actual_finish, mode: :finish))

        current_instance = nil

        loop do
          if current_instance
            working_ds = base_ds.where(conditions.call(current_instance.to_h, including: false))
          else
            working_ds = base_ds
          end

          current_instance = db.from(working_ds.limit(of)).select(*pk).order(*pk).last or break
          working_ds = working_ds.where(conditions.call(current_instance.to_h, mode: :finish))

          yield working_ds
        end
      end

      private

      ::Sequel::Dataset.register_extension(:batches, Batches)
    end
  end
end
