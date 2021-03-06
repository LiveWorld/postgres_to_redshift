# PostgresToRedshift

This gem copies data from postgres to redshift. It's especially useful to copy data from postgres to redshift in heroku.

[![Build Status](https://travis-ci.org/kitchensurfing/postgres_to_redshift.svg?branch=master)](https://travis-ci.org/kitchensurfing/postgres_to_redshift)

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'postgres_to_redshift'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install postgres_to_redshift

## Usage

Set your source and target databases, as well as your s3 intermediary.

```bash
export POSTGRES_TO_REDSHIFT_SOURCE_URI='postgres://username:password@host:port/database-name'
export POSTGRES_TO_REDSHIFT_TARGET_URI='postgres://username:password@host:port/database-name'
export POSTGRES_TO_REDSHIFT_TARGET_SCHEMA='testing-data'
export S3_DATABASE_EXPORT_ID='yourid'
export S3_DATABASE_EXPORT_KEY='yourkey'
export S3_DATABASE_EXPORT_BUCKET='some-bucket-to-use'
export REDSHIFT_INCLUDE_TABLES='table-pattern-to-include1,table-pattern-to-include2'

postgres_to_redshift
```

### Incremental Imports

It is possible to run an import that will pick up only records that have updated sine the last run of the import. It has the following caveats:

1. Does not apply deletions to the target table
1. Requires that the source table has either an `updated_at` or `created_at` field

Should you wish to enable incremental mode, set the following ENV:

```bash
export POSTGRES_TO_REDSHIFT_INCREMENTAL=true
```

It will record the start time of the last import in a local file and will import changes on or after that start time for subsequent imports.

For tables that cannot run incrementally, please specify the tables in this variable, and they will always be run in "full" mode:

```bash
export TABLES_ALWAYS_FULL_IMPORT='table-that-has-no-primary-id,table-that-has-no-updated_at'
```

### Dry Runs

It is possible to run the import in _dry run_ mode whereby the import will run, but will roll back any changes to the target tables.

```bash
export POSTGRES_TO_REDSHIFT_DRY_RUN=true
```

### Error handling

If an error is encountered during an import it will be handled as follows:

* Incremental imports: The import will be rolled back and retried from the beginning
* Full imports: The import will be rolled back to the previous table and the current table's import will be retried

An import will be attempted three times before giving up and raising the exception to the caller.

#### Transactions

For an _incremental_ import, the entire import process is performed in one database transaction to ensure that the data remains in a consistent state while the import is running as it is assumed that the incremental import will be running during business hours moving a relatively small amount of data. For a _full_ import, each table is imported in its own transaction as it is assumed that the full import is running outside of business hours and would be moving too large a volume of data to be performed in a single transaction.

## Contributing

1. Fork it ( https://github.com/kitchensurfing/postgres_to_redshift/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request
