RSpec.describe Sequel::Extensions::Batches do
  before(:all) do
    DB.extension :batches
    DB.extension :pg_array
  end

  it "has a version number" do
    expect(Sequel::Extensions::Batches::VERSION).not_to be nil
  end

  it "splits 6 records in 2 chunks" do
    chunks = []
    DB[:data].in_batches(of:3) { |b| chunks << b.select_map(:id) }
    expect(chunks[0]).to match_array([1,2,3])
    expect(chunks[1]).to match_array([4,5,6])
  end

  it "splits 6 records in 3 chunks" do
    chunks = []
    DB[:data].in_batches(of:2) { |b| chunks << b.select_map(:id) }
    expect(chunks[0]).to match_array([1,2])
    expect(chunks[1]).to match_array([3,4])
    expect(chunks[2]).to match_array([5,6])
  end

  it "splits 6 records in 6 chunks" do
    chunks = []
    DB[:data].in_batches(of:1) { |b| chunks << b.select_map(:id) }
    expect(chunks[0]).to match_array([1])
    expect(chunks[1]).to match_array([2])
    expect(chunks[2]).to match_array([3])
    expect(chunks[3]).to match_array([4])
    expect(chunks[4]).to match_array([5])
    expect(chunks[5]).to match_array([6])
  end

  it "starts from 4" do
    chunks = []
    DB[:data].in_batches(of:1, start: {id: 4}) { |b| chunks << b.select_map(:id) }
    expect(chunks[0]).to match_array([4])
    expect(chunks[1]).to match_array([5])
    expect(chunks[2]).to match_array([6])
  end

  it "ends on 3" do
    chunks = []
    DB[:data].in_batches(of:1, finish: {id: 3}) { |b| chunks << b.select_map(:id) }
    expect(chunks[0]).to match_array([1])
    expect(chunks[1]).to match_array([2])
    expect(chunks[2]).to match_array([3])
  end

  it "uses another column" do
    chunks = []
    DB[:data].in_batches(pk: [:created_at], of:1, start: {created_at: "2017-05-01"}) { |b| chunks << b.select_map(:id) }
    expect(chunks.flatten).to match_array([3, 4, 5, 6])
  end

  it "works correctly composite" do
    chunks = []
    DB[:data].in_batches(pk: [:id, :value], of: 1) { |b| chunks << b.select_map(:id) }
    expect(chunks.flatten).to match_array([1, 2, 3, 4, 5, 6])
  end
end
