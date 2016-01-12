require 'vertica'

module Sequel
  module Vertica
    module ErrorHandler
      DATABASE_ERROR_REGEXPS = {
          /Sqlstate: 22004/ => NotNullConstraintViolation,
      }.freeze

      def database_error_regexps
        DATABASE_ERROR_REGEXPS
      end
    end

    class Database < Sequel::Database
      include ErrorHandler

      set_adapter_scheme :vertica

      def connect(server)
        connection_options = server_opts(server)

        Connection.new(connection_options)
      end

      def execute(sql, opts = {}, &block)
        res = nil
        synchronize(opts[:server]) do |conn|
          raise DatabaseConnectionError, 'Connection to server was closed.' if conn.closed?

          res = log_yield(sql, opts[:arguments]) { conn.query(sql) }
          block.call(res) if block_given?
        end
        res
      rescue ::Vertica::Error => e
        raise_error(e)
      end

      def execute_insert(sql, opts = {}, &block)
        execute(sql, opts, &block)

        result = execute('SELECT LAST_INSERT_ID()')
        result.rows.first[:LAST_INSERT_ID]
      end

      def execute_dui(sql, opts = OPTS)
        result = execute(sql, opts)

        if result
          result.rows.first ? result.first[:OUTPUT] : nil
        end
      end

      def log_connection_execute(conn, sql)
        raise DatabaseConnectionError, 'Connection to server was closed.' if conn.closed?

        log_yield(sql) { conn.execute(sql) }
      end

      def create_table_generator_class
        Sequel::Vertica::CreateTableGenerator
      end

      def auto_increment_sql
        'AUTO_INCREMENT'
      end

      # Vertica is case sensitive DB, so don't need to upcase identifiers
      def identifier_input_method_default
        nil
      end

      # Vertica is case sensitive DB, so don't need to upcase identifiers
      def identifier_output_method_default
        nil
      end

      def type_literal_generic_file(column)
        :'varbinary(65000)'
      end

      def type_literal_generic_string(column)
        column[:text] ? 'varchar(65000)' : super
      end

      def supports_create_table_if_not_exists?
        true
      end

      def supports_drop_table_if_exists?
        true
      end

      def supports_transaction_isolation_levels?
        true
      end

      def tables(options = OPTS)
        output_identifier = output_identifier_meth(options[:dataset])

        schema = options[:schema]
        filter = {}
        filter[:table_schema] = schema.to_s if schema

        metadata_dataset
          .select(:table_name)
          .from(:v_catalog__tables)
          .filter(filter)
          .map { |row| output_identifier.call(row[:table_name]) }
      end

      def schema_parse_table(table_name, options = {})
        output_identifier = output_identifier_meth(options[:dataset])
        input_identifier = input_identifier_meth(options[:dataset])

        table_name = input_identifier.call(table_name)
        schema = options[:schema]

        selector = [
          :columns__column_name,
          :constraint_type,
          :is_nullable,
          :column_default,
          :data_type,
          :is_identity
        ]

        filter = { columns__table_name: table_name }
        filter[:columns__table_schema] = schema.to_s if schema

        dataset = metadata_dataset
          .select(*selector)
          .filter(filter)
          .from(:v_catalog__columns)
          .left_outer_join(:v_catalog__constraint_columns, table_id: :table_id, column_name: :column_name, constraint_type: 'p')

        dataset.map do |row|
          row[:allow_null] = row.delete(:is_nullable)
          row[:default] = row.delete(:column_default)
          row[:db_type] = row.delete(:data_type)
          row[:auto_increment] = row.delete(:is_identity)

          row[:default] = nil if blank_object?(row[:default])
          row[:type] = schema_column_type(row[:db_type])
          row[:primary_key] = row.delete(:constraint_type) == 'p'

          column_name = row.delete(:column_name)
          column_name = output_identifier.call(column_name)

          [ column_name.to_sym, row ]
        end
      end

      def alter_table_set_column_type_sql(table, op)
        "ALTER COLUMN #{quote_identifier(op[:name])} SET DATA TYPE #{type_literal(op)}"
      end

      def begin_transaction(conn, opts = OPTS)
        log_yield(TRANSACTION_BEGIN) do
          conn.execute('SET SESSION AUTOCOMMIT TO OFF')
          conn.execute('BEGIN')
        end
      end

      def commit_transaction(conn, opts = OPTS)
        log_yield(TRANSACTION_COMMIT) do
          conn.execute('COMMIT')
        end
      end

      def rollback_transaction(conn, opts = OPTS)
        log_yield(TRANSACTION_ROLLBACK) do
          conn.execute('ROLLBACK')
        end
      end

      def remove_transaction(conn, committed)
        conn.execute('SET SESSION AUTOCOMMIT TO ON')
      ensure
        super
      end

    end

    class Connection
      def initialize(connection_options)
        @connection = ::Vertica::Connection.new(connection_options)
        @connection.query('SET SESSION AUTOCOMMIT TO ON')
      end

      def query(sql)
        @connection.query(sql)
      end

      def execute(sql)
        @connection.query(sql)
      end

      def close
        @connection.close
      end

      def closed?
        @connection.closed?
      end
    end

    class CreateTableGenerator < Sequel::Schema::CreateTableGenerator
      def primary_key(name, *args)
        super

        if @primary_key && @primary_key[:auto_increment]
          @primary_key.delete(:auto_increment)
          @primary_key[:type] = 'AUTO_INCREMENT'
        end
      end
    end

    class Dataset < Sequel::Dataset
      Database::DatasetClass = self

      def supports_timestamp_timezones?
        true
      end

      def supports_limits_in_correlated_subqueries?
        false
      end

      def supports_multiple_column_in?
        false
      end

      def fetch_rows(sql)
        tz = db.timezone if Sequel.application_timezone
        execute(sql) do |result|
          @columns ||= result.columns.map { |c| identifier_output_method ? output_identifier(c.name) : c.name }

          result.each do |row|
            row = row.reduce({}) do |hash, (k, v)|
              hash.tap do
                k = output_identifier(k) if identifier_output_method
                hash[k] = case v
                  when DateTime
                    tz ? Sequel.database_to_application_timestamp(Sequel.send(:convert_input_datetime_no_offset, v, tz)) : v
                  else
                    v
                end
              end
            end

            yield row
          end
        end
      end

      def complex_expression_sql_append(sql, op, args)
        case op
          when :^
            complex_expression_arg_pairs_append(sql, args) { |a, b| Sequel.lit(['', ' # ', ''], a, b) }
          else
            super(sql, op, args)
        end
      end

      def literal_blob_append(sql, v)
        sql << %{HEX_TO_BINARY('0x} << v.unpack('H*').first << %{')}
      end

    end

  end
end
