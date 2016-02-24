SEQUEL_ADAPTER_TEST = :vertica

require File.join(File.dirname(File.expand_path(__FILE__)), 'spec_helper.rb')

def DB.sqls
  (@sqls ||= [])
end
logger = Object.new
def logger.method_missing(m, msg)
  DB.sqls << msg
end
DB.loggers << logger


describe 'Vertica', '#create_table' do
  before do
    @db = DB
    @db.test_connection
    DB.sqls.clear
  end

  after do
    @db.drop_table?(:items)
  end

  it 'should create table with SEGEMNTED BY and PARTITION BY options' do
    @db.create_table(:items, :segmented_by => 'HASH(requested_day) ALL NODES', :partition_by => 'requested_day') {
      column :requested_day, :varchar, :null => false
    }

    check_sqls do
      @db.sqls.must_equal ['CREATE TABLE "items" ("requested_day" varchar(255) NOT NULL) SEGMENTED BY HASH(requested_day) ALL NODES PARTITION BY requested_day']
    end
  end
end

describe 'Vertica', 'copy' do
  before(:all) do
    @db = DB
    @db.create_table(:items) {
      column :value1, :varchar
      column :value2, :integer
    }
    @ds = @db[:items]
  end

  before do
    @ds.delete
  end

  after do
    @db.drop_table?(:items)
  end

  it 'COPY FROM STDIN' do
    @db.copy(%{COPY items (value1, value2) FROM STDIN DELIMITER ','}) do |stdin|
      stdin << "100500,100600\n"
    end

    @ds.count.must_equal(1)
    @ds.first.must_equal({ value1: '100500', value2: 100600 })
  end
end
