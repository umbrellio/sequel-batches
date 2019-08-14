RSpec.describe Sequel::Extensions::Batches do
  before(:all) do
    DB.extension :batches
    DB.extension :pg_array
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

  it "works correctly composite on reversed pk" do
    DB[:data].in_batches(pk: [:value, :id], of: 2) { |b| chunks << b.select_map(:id) }
    expect(chunks).to eq([[1, 2], [3, 4], [5, 6]])
  end

  it "works correctly with real composite pk" do
    DB[:points].in_batches { |b| chunks << b.select_map([:x, :y, :z]) }
    expect(chunks).to eq([[[15, 15, 15], [15, 20, 20]]])
  end

  it "works correctly with real composite pk and small of" do
    DB[:points].in_batches(of: 1) { |b| chunks << b.select_map([:x, :y, :z]) }
    expect(chunks).to eq([[[15, 15, 15]], [[15, 20, 20]]])
  end

  it "works with updating recors" do
    DB[:data].in_batches { |b| b.update(created_at: "2019-01-01") }
    expect(DB[:data].where(created_at: "2019-01-01").count).to eq(6)
  end
end
