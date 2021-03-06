require 'pg'
require 'uri'
require 'aws-sdk-s3'
require 'zlib'
require 'tempfile'
require 'time'
require 'postgres_to_redshift/table'
require 'postgres_to_redshift/column'
require 'postgres_to_redshift/copy_import'
require 'postgres_to_redshift/full_import'
require 'postgres_to_redshift/incremental_import'
require 'postgres_to_redshift/update_tables'
require 'postgres_to_redshift/version'
require 'slack-notifier'

module PostgresToRedshift
  TIMESTAMP_FILE_NAME = 'POSTGRES_TO_REDHSIFT_TIMESTAMP'.freeze
  extend self

  def update_tables
    notifier.ping "Postgresql_to_Redshift has started from source #{ENV['POSTGRES_TO_REDSHIFT_SOURCE_URI']}.", icon_emoji: ENV["SLACK_ICON_EMOJI"]
    update_tables = UpdateTables.new(bucket: bucket, s3: s3, source_uri: source_uri, target_uri: target_uri, schema: schema)
    incremental? ? update_tables.incremental : update_tables.full
    cleanup
  end

  def cleanup
    puts "Deleting psv.gz files left in the S3 bucket."
    bucket.objects({prefix: 'export'}).batch_delete!
    notifier.ping "Postgresql_to_Redshift has finished, now deleting psv.gz files left in the S3 bucket.", icon_emoji: ENV["SLACK_ICON_EMOJI"]
  end

  def dry_run?
    ENV['POSTGRES_TO_REDSHIFT_DRY_RUN'] == 'true'
  end

  private

  def incremental?
    ENV['POSTGRES_TO_REDSHIFT_INCREMENTAL'] == 'true' && File.exist?(TIMESTAMP_FILE_NAME)
  end

  def source_uri
    @source_uri ||= URI.parse(ENV.fetch('POSTGRES_TO_REDSHIFT_SOURCE_URI'))
  end

  def target_uri
    @target_uri ||= URI.parse(ENV.fetch('POSTGRES_TO_REDSHIFT_TARGET_URI'))
  end

  def schema
    ENV.fetch('POSTGRES_TO_REDSHIFT_TARGET_SCHEMA')
  end

  def s3
    @s3 ||= Aws::S3::Client.new(access_key_id: ENV.fetch('S3_DATABASE_EXPORT_ID'), secret_access_key: ENV.fetch('S3_DATABASE_EXPORT_KEY'))
  end

  def bucket
    @bucket ||= Aws::S3::Bucket.new(ENV['S3_DATABASE_EXPORT_BUCKET'], client: s3)
  end

  def notifier
    @notifier ||= Slack::Notifier.new(ENV["SLACK_WEBHOOK_URL"], channel: ENV["SLACK_CHANNEL"], username: ENV["SLACK_USERNAME"])
  end
end
