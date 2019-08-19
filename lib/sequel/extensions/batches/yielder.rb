# frozen_string_literal: true

module Sequel
  module Extensions
    module Batches
      class Yielder
        attr_accessor :ds, :pk, :of, :start, :finish

        def initialize(ds:, pk: nil, of: 1000, start: nil, finish: nil)
          self.ds = ds

          self.pk = pk
          self.pk ||= db.schema(ds.first_source).select { |x| x[1][:primary_key] }.map(&:first)

          self.of = of
          self.start = start
          self.finish = finish
        end

        def call
          raise MissingPKError if pk.empty?

          qualified_pk = pk.map { |x| Sequel[ds.first_source][x] }

          check_pk = lambda do |input_pk|
            raise InvalidPKError if input_pk.keys != pk

            input_pk
          end

          conditions = lambda do |pk, sign:|
            raise NullPKError if pk.values.any?(&:nil?)

            row_expr = Sequel.function(:row, *pk.values)
            Sequel.function(:row, *qualified_pk).public_send(sign, row_expr)
          end

          base_ds = ds.order(*qualified_pk)
          base_ds = base_ds.where(conditions.call(check_pk.call(start), sign: :>=)) if start
          base_ds = base_ds.where(conditions.call(check_pk.call(finish), sign: :<=)) if finish

          pk_ds = db.from(base_ds).select(*pk).order(*pk)
          actual_start = pk_ds.first
          actual_finish = pk_ds.last

          return unless actual_start && actual_finish

          base_ds = base_ds.where(conditions.call(actual_start, sign: :>=))
          base_ds = base_ds.where(conditions.call(actual_finish, sign: :<=))

          current_instance = nil

          loop do
            working_ds =
              if current_instance
                base_ds.where(conditions.call(current_instance.to_h, sign: :>))
              else
                base_ds
              end

            current_instance = db.from(working_ds.limit(of)).select(*pk).order(*pk).last or break
            working_ds = working_ds.where(conditions.call(current_instance.to_h, sign: :<=))

            yield working_ds
          end
        end

        private

        def db
          ds.db
        end
      end
    end
  end
end
