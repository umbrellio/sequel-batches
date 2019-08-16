RSpec.describe Sequel::Extensions::Batches do
  before(:all) do
    DB.extension :batches
  end

  let(:chunks) { [] }

  it "has a version number" do
    expect(Sequel::Extensions::Batches::VERSION).not_to be nil
  end

  it "splits 6 records in 2 chunks" do
    DB[:data].in_batches(of: 3) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([[1,2,3], [4,5,6]])
  end

  it "splits 6 records in 3 chunks" do
    DB[:data].in_batches(of: 2) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([[1,2], [3,4], [5,6]])
  end

  it "splits 6 records in 6 chunks" do
    DB[:data].in_batches(of: 1) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([[1], [2], [3], [4], [5], [6]])
  end

  it "starts from 4" do
    DB[:data].in_batches(of: 1, start: {id: 4}) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([[4], [5], [6]])
  end

  it "ends on 3" do
    DB[:data].in_batches(of: 1, finish: {id: 3}) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([[1], [2], [3]])
  end

  it "uses another column" do
    DB[:data].in_batches(pk: [:created_at], start: { created_at: "2017-05-01" } ) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([[3, 4, 5, 6]])
  end

  it "works correctly with composite pk" do
    DB[:data].in_batches(pk: [:id, :value], of: 3) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([[1, 2, 3], [4, 5, 6]])
  end

  it "works correctly with composite pk on reversed pk" do
    DB[:data].in_batches(pk: [:value, :id], of: 2) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([[1, 2], [3, 4], [5, 6]])
  end

  it "raises NullPKError in case of pk containing nulls" do
    DB[:data].where(id: [1, 2, 3]).update(value: nil, created_at: nil)

    expect { DB[:data].in_batches(pk: [:id, :value, :created_at], of: 3) {} }
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
    DB[:data].in_batches(start: {id: 100}) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([])
  end

  it "works correctly with real composite pk" do
    DB[:points].in_batches(pk: %i[x y z]) { |b| chunks << b.select_map([:x, :y, :z]) }
    expect(chunks).to eq([[[15, 15, 15], [15, 20, 20]]])
  end

  it "works correctly with real composite pk and small of" do
    DB[:points].in_batches(pk: %i[x y z], of: 1) { |b| chunks << b.select_map([:x, :y, :z]) }
    expect(chunks).to eq([[[15, 15, 15]], [[15, 20, 20]]])
  end

  it "raises InvalidPKError in case of incorrect key ordering in start" do
    expect { DB[:points].in_batches(pk: %i[x y z], start: {y: 16, z: 100, x: 15}) {} }
      .to raise_error(Sequel::Extensions::Batches::InvalidPKError)
  end

  it "raises MissingPKError in case of missing pk" do
    expect { DB[:points].in_batches {} }.to raise_error(Sequel::Extensions::Batches::MissingPKError)
  end
end
