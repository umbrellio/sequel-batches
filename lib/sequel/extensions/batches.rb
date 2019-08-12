require "sequel/extensions/batches/version"
require "sequel/model"

module Sequel
  module Extensions
    module Batches
      MissingPKError = Class.new(StandardError)

      def in_batches(pk: nil, of: 1000, start: {}, finish: {})
        pk ||=
          db.schema(first_source)
            .select { |x| x[1][:primary_key] }
            .map(&:first) or raise MissingPKError

        qualified_pk = pk.map { |x| Sequel[first_source][x] }
        pk_combinations = pk.map.with_index { |x, i| pk[0..i] }

        # For composite PK (x, y, z) this will generate the following WHERE expression:
        # (x > ?) OR (x = ? AND y > ?) OR (x = ? AND y = ? AND z > ?)
        generate_conditions = lambda do |values, start: false, finish: false|
          or_conditions = pk_combinations.map do |keys|
            and_conditions = keys.map.with_index do |key, index|
              value = values.fetch(key)

              # All conditions should use equality except for the last one
              if index == keys.size - 1
                case
                when start
                  Sequel[key] >= value
                when finish
                  Sequel[key] <= value
                else
                  Sequel[key] > value
                end
              else
                Sequel[key] =~ value
              end
            end

            and_conditions.reduce(:&)
          end

          or_conditions.reduce(:|)
        end

        base_ds = order(*qualified_pk).limit(of)
        base_ds = base_ds.where(generate_conditions.call(start, start: true)) if start.present?
        base_ds = base_ds.where(generate_conditions.call(finish, finish: true)) if finish.present?

        last_instance = nil

        loop do
          if last_instance
            ds = base_ds.where(generate_conditions.call(last_instance.to_h))
          else
            ds = base_ds
          end

          last_instance = db.from(ds).select(*pk).order(*pk).last or break

          yield ds
        end
      end

      private

      ::Sequel::Dataset.register_extension(:batches, Batches)
    end
  end
end
