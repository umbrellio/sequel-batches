# frozen_string_literal: true

RSpec.describe "Sequel::Extensions::Batches" do
  let(:chunks) { [] }

  it "raises InvalidPKError in case of incorrect key ordering in start" do
    expect { DB[:points].in_batches(pk: %i[x y z], start: { y: 16, z: 100, x: 15 }) }
      .to raise_error(Sequel::Extensions::Batches::InvalidPKError)
  end

  it "raises MissingPKError in case of missing pk" do
    expect { DB[:points].in_batches }.to raise_error(Sequel::Extensions::Batches::MissingPKError)
  end

  it "validates order option" do
    expect { DB[:data].in_batches(of: 3, order: :wrong) }.to raise_error(ArgumentError)
  end

  it "raises ArgumentError on unknown options" do
    expect { DB[:data].in_batches(wrong: :argument) }.to raise_error(ArgumentError)
  end

  it "splits 6 records in 2 chunks" do
    DB[:data].in_batches(of: 3) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([[1, 2, 3], [4, 5, 6]])
  end

  it "splits 6 records in 3 chunks" do
    DB[:data].in_batches(of: 2) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([[1, 2], [3, 4], [5, 6]])
  end

  it "splits 6 records in 6 chunks" do
    DB[:data].in_batches(of: 1) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([[1], [2], [3], [4], [5], [6]])
  end

  it "starts from 4" do
    DB[:data].in_batches(of: 1, start: { id: 4 }) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([[4], [5], [6]])
  end

  it "ends on 3" do
    DB[:data].in_batches(of: 1, finish: { id: 3 }) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([[1], [2], [3]])
  end

  it "uses another column" do
    DB[:data].in_batches(pk: [:created_at], start: { created_at: "2017-05-01" }) do |b|
      chunks << b.select_map(:id)
    end

    expect(chunks).to eq([[3, 4, 5, 6]])
  end

  it "works correctly with composite pk" do
    DB[:data].in_batches(pk: %i[id value], of: 3) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([[1, 2, 3], [4, 5, 6]])
  end

  it "works correctly with composite pk on reversed pk" do
    DB[:data].in_batches(pk: %i[value id], of: 2) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([[1, 2], [3, 4], [5, 6]])
  end

  it "raises NullPKError in case of pk containing nulls" do
    DB[:data].where(id: [1, 2, 3]).update(value: nil, created_at: nil)

    expect { DB[:data].in_batches(pk: %i[id value created_at], of: 3) }
      .to raise_error(Sequel::Extensions::Batches::NullPKError)
  end

  it "works with updating records" do
    DB[:data].in_batches { |b| b.update(created_at: "2019-01-01") }
    expect(DB[:data].where(created_at: "2019-01-01").count).to eq(6)
  end

  it "does nothing if table is empty" do
    DB[:data].delete
    DB[:data].in_batches { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([])
  end

  it "does nothing if start is too high" do
    DB[:data].in_batches(start: { id: 100 }) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([])
  end

  it "works correctly with real composite pk" do
    DB[:points].in_batches(pk: %i[x y z]) { |b| chunks << b.select_map(%i[x y z]) }
    expect(chunks).to eq([[[15, 15, 15], [15, 20, 20]]])
  end

  it "works correctly with real composite pk and small of" do
    DB[:points].in_batches(pk: %i[x y z], of: 1) { |b| chunks << b.select_map(%i[x y z]) }
    expect(chunks).to eq([[[15, 15, 15]], [[15, 20, 20]]])
  end

  it "qualifies pk to mitigate ambiguous column error" do
    expect { DB[:data, :data2].in_batches }.not_to raise_error
  end

  it "respects order option" do
    DB[:data].in_batches(of: 3, order: :desc) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([[6, 5, 4], [3, 2, 1]])
  end

  it "respects order option with composite pk" do
    DB[:points].in_batches(pk: %i[x y z], order: :desc).each { |b| chunks << b.select_map(%i[x y z]) }
    expect(chunks).to eq([[[15, 20, 20], [15, 15, 15]]])
  end
end
