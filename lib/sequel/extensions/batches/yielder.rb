# frozen_string_literal: true

module Sequel::Extensions::Batches
  class Yielder
    attr_accessor :ds, :of, :start, :finish
    attr_writer :pk

    def initialize(ds:, pk: nil, of: 1000, start: nil, finish: nil)
      self.ds = ds
      self.pk = pk
      self.of = of
      self.start = start
      self.finish = finish
    end

    def call
      base_ds = setup_base_ds or return

      current_instance = nil

      loop do
        working_ds =
          if current_instance
            base_ds.where(generate_conditions(current_instance.to_h, sign: :>))
          else
            base_ds
          end

        working_ds_pk = working_ds.select(*qualified_pk).limit(of)
        current_instance = db.from(working_ds_pk).select(*pk).order(*pk).last or break
        working_ds = working_ds.where(generate_conditions(current_instance.to_h, sign: :<=))

        yield working_ds
      end
    end

    private

    def db
      ds.db
    end

    def pk
      @pk ||= begin
        pk = db.schema(ds.first_source).select { |x| x[1][:primary_key] }.map(&:first)
        raise MissingPKError if pk.empty?
        pk
      end
    end

    def qualified_pk
      @qualified_pk ||= pk.map { |x| Sequel[ds.first_source][x] }
    end

    def check_pk(input_pk)
      raise InvalidPKError if input_pk.keys != pk
      input_pk
    end

    def generate_conditions(input_pk, sign:)
      raise NullPKError if input_pk.values.any?(&:nil?)
      row_expr = Sequel.function(:row, *input_pk.values)
      Sequel.function(:row, *qualified_pk).public_send(sign, row_expr)
    end

    def setup_base_ds
      base_ds = ds.order(*qualified_pk)
      base_ds = base_ds.where(generate_conditions(check_pk(start), sign: :>=)) if start
      base_ds = base_ds.where(generate_conditions(check_pk(finish), sign: :<=)) if finish

      pk_ds = db.from(base_ds.select(*qualified_pk)).select(*pk).order(*pk)
      actual_start = pk_ds.first
      actual_finish = pk_ds.last

      return unless actual_start && actual_finish

      base_ds = base_ds.where(generate_conditions(actual_start, sign: :>=))
      base_ds = base_ds.where(generate_conditions(actual_finish, sign: :<=))

      base_ds
    end
  end
end
