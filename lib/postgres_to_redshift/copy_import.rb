module PostgresToRedshift
  class CopyImport
    KILOBYTE = 1024
    MEGABYTE = KILOBYTE * 1024
    GIGABYTE = MEGABYTE * 1024
    CHUNK_SIZE = 3 * GIGABYTE
    BEGINNING_OF_TIME = Time.at(0).utc

    def initialize(table:, s3:, bucket:, source_connection:, target_connection:, schema:, incremental_from: BEGINNING_OF_TIME, incremental_to:)
      @table = table
      @s3 = s3
      @bucket = bucket
      @source_connection = source_connection
      @target_connection = target_connection
      @schema = schema
      @incremental_from = incremental_from
      @incremental_to = incremental_to
    end

    def run
      copy_table
      import_table
    end

    private

    def select_sql
      select_sql = "SELECT #{table.columns_for_copy} FROM #{table.name}"
      select_sql += " WHERE #{incremental_column} BETWEEN '#{incremental_from.iso8601}' AND '#{incremental_to.iso8601}'" if incremental_column
      select_sql
    end

    def incremental_column
      @incremental_column ||= %w[updated_at created_at].detect { |column_name| table.column_names.include?(column_name) }
    end

    def new_tmpfile
      tmpfile = StringIO.new
      tmpfile.set_encoding('utf-8')
      tmpfile.binmode
      tmpfile
    end

    def start_chunk
      tmpfile = new_tmpfile
      zip = Zlib::GzipWriter.new(tmpfile)
      [tmpfile, zip]
    end

    def close_resources(zip:)
      zip.close unless zip.closed?
    end

    def finish_chunk(tmpfile:, zip:, chunk:, options:)
      zip.finish
      tmpfile.rewind
      upload_table(tmpfile, chunk, options)
      close_resources(zip: zip)
    end

    def copy_table
      tmpfile, zip = start_chunk
      chunk = 1
      bucket.objects({prefix: "export/#{table.target_table_name}.psv.gz"}).batch_delete!
      begin
        puts "Downloading #{table} changes between #{incremental_from} and #{incremental_to} at #{Time.now.utc}"
        copy_command = "COPY (#{select_sql}) TO STDOUT WITH DELIMITER '|'"
        key = "export/#{table.target_table_name}.psv.gz"
        multipart_upload = s3.create_multipart_upload(bucket: bucket.name, key: key)

        options = {
          bucket: bucket.name,
          key: key,
          upload_id: multipart_upload.upload_id
        }

        source_connection.copy_data(copy_command) do
          while (row = source_connection.get_copy_data)
            zip.write(row)
            next unless zip.pos > CHUNK_SIZE

            finish_chunk(tmpfile: tmpfile, zip: zip, chunk: chunk, options: options)
            chunk += 1
            tmpfile, zip = start_chunk
          end
        end

        finish_chunk(tmpfile: tmpfile, zip: zip, chunk: chunk, options: options)


        all_parts = s3.list_parts(options)

        options.merge!(
          multipart_upload: {
            parts:
              all_parts.parts.map do |part|
                { part_number: part.part_number, etag: part.etag }
              end
          }
        )

        s3.complete_multipart_upload(options)

        source_connection.reset
      ensure
        close_resources(zip: zip)
      end
    end

    def upload_table(buffer, chunk, options)
      puts "Uploading #{table.target_table_name}.#{chunk}"
      #bucket.objects["export/#{table.target_table_name}.psv.gz.#{chunk}"].write(buffer, acl: :authenticated_read)
      s3.upload_part(
        body:        buffer,
        bucket:      options[:bucket],
        key:         options[:key],
        part_number: chunk,
        upload_id:   options[:upload_id]
      )
    end

    def import_table
      tables_for_full = ENV.fetch('TABLES_ALWAYS_FULL_IMPORT','').split(',').map(&:downcase)
      args = { table: table, target_connection: target_connection, schema: schema }

      if incremental? && !tables_for_full.include?(table.to_s.strip)
        puts "Doing incremental update for #{table}"
        import = IncrementalImport.new(**args)
      else
        puts "Doing full import for #{table}"
        import = FullImport.new(**args)
      end

      import.run
    end

    def incremental?
      incremental_from != BEGINNING_OF_TIME
    end

    attr_reader :table, :s3, :bucket, :source_connection, :target_connection, :schema, :incremental_from, :incremental_to
  end
end
