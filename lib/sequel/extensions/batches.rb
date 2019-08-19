module Sequel
  module Extensions
    module Batches
      MissingPKError = Class.new(StandardError)
      NullPKError = Class.new(StandardError)
      InvalidPKError = Class.new(StandardError)

      def in_batches(pk: nil, of: 1000, start: nil, finish: nil)
        pk ||= db.schema(first_source).select { |x| x[1][:primary_key] }.map(&:first)
        raise MissingPKError if pk.empty?

        qualified_pk = pk.map { |x| Sequel[first_source][x] }

        check_pk = lambda do |input_pk|
          raise InvalidPKError if input_pk.keys != pk
          input_pk
        end

        conditions = lambda do |pk, sign:|
          raise NullPKError if pk.values.any?(&:nil?)
          row_expr = Sequel.function(:row, *pk.values)
          Sequel.function(:row, *qualified_pk).public_send(sign, row_expr)
        end

        base_ds = order(*qualified_pk)
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
          if current_instance
            working_ds = base_ds.where(conditions.call(current_instance.to_h, sign: :>))
          else
            working_ds = base_ds
          end

          current_instance = db.from(working_ds.limit(of)).select(*pk).order(*pk).last or break
          working_ds = working_ds.where(conditions.call(current_instance.to_h, sign: :<=))

          yield working_ds
        end
      end

      private

      ::Sequel::Dataset.register_extension(:batches, Batches)
    end
  end
end
