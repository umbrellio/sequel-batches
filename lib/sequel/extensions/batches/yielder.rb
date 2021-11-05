# frozen_string_literal: true

module Sequel::Extensions::Batches
  class Yielder
    attr_accessor :ds, :of, :start, :finish, :order
    attr_writer :pk

    def initialize(ds:, pk: nil, of: 1000, start: nil, finish: nil, order: :asc)
      self.ds = ds
      self.pk = pk
      self.of = of
      self.start = start
      self.finish = finish

      unless [:asc, :desc].include?(order)
        raise ArgumentError, ":order must be :asc or :desc, got #{order}"
      end
      self.order = order
    end

    def call
      base_ds = setup_base_ds or return

      current_instance = nil

      loop do
        working_ds =
          if current_instance
            base_ds.where(generate_conditions(current_instance.to_h, sign: sign_from_exclusive))
          else
            base_ds
          end

        working_ds_pk = working_ds.select(*qualified_pk).order(order_by).limit(of)
        current_instance = db.from(working_ds_pk).select(*pk).order(order_by).last or break
        working_ds = working_ds.where(generate_conditions(current_instance.to_h, sign: sign_to_inclusive))

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

    def asc_order?
      order == :asc
    end

    def sign_from_exclusive
      asc_order? ? :> : :<
    end

    def sign_from_inclusive
      asc_order? ? :>= : :<=
    end

    def sign_to_inclusive
      asc_order? ? :<= : :>=
    end

    def order_by(qualified: false)
      columns = qualified ? qualified_pk : pk
      asc_order? ? Sequel.asc(columns) : Sequel.desc(columns)
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
      base_ds = ds.order(order_by(qualified: true))
      base_ds = base_ds.where(generate_conditions(check_pk(start), sign: sign_from_inclusive)) if start
      base_ds = base_ds.where(generate_conditions(check_pk(finish), sign: sign_to_inclusive)) if finish

      pk_ds = db.from(base_ds.select(*qualified_pk)).select(*pk).order(order_by)
      actual_start = pk_ds.first
      actual_finish = pk_ds.last

      return unless actual_start && actual_finish

      base_ds = base_ds.where(generate_conditions(actual_start, sign: sign_from_inclusive))
      base_ds = base_ds.where(generate_conditions(actual_finish, sign: sign_to_inclusive))

      base_ds
    end
  end
end
