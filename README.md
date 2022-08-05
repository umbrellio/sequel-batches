# Sequel::Batches    [![Gem Version](https://badge.fury.io/rb/sequel-batches.svg)](https://badge.fury.io/rb/sequel-batches) [![Build Status](https://travis-ci.org/umbrellio/sequel-batches.svg?branch=master)](https://travis-ci.org/umbrellio/sequel-batches) [![Coverage Status](https://coveralls.io/repos/github/umbrellio/sequel-batches/badge.svg?branch=master)](https://coveralls.io/github/umbrellio/sequel-batches?branch=master)

This dataset extension provides the `#in_batches` method. The method splits dataset in parts and yields it.

Note: currently only PostgreSQL database is supported.

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'sequel-batches'
```

## Usage

In order to use the feature you should enable the extension:

```ruby
Sequel::DATABASES.first.extension :batches
```

After that the `#in_batches` method becomes available on dataset:

```ruby
User.where(role: "admin").in_batches(of: 4) do |ds|
  ds.delete
end
```

Finally, here's an example including all the available options:

```ruby
options = {
  of: 4,
  pk: [:project_id, :external_user_id],
  start: { project_id: 2, external_user_id: 3 },
  finish: { project_id: 5, external_user_id: 70 },
  order: :desc,
}

Event.where(type: "login").in_batches(options) do |ds|
  ds.delete
end
```

## Options

You can set the following options:

### pk

Overrides primary key of your dataset. This option is required in case your table doesn't have a real PK, otherwise you will get `Sequel::Extensions::Batches::MissingPKError`.

Note that you have to provide columns that don't contain NULL values, otherwise this may not work as intended. You will receive `Sequel::Extensions::Batches::NullPKError` in case batch processing detects a NULL value on it's way, but it's not guaranteed since it doesn't check all the rows for performance reasons.

### of

Sets chunk size (1000 by default).

### start

A hash `{ [column]: <start_value> }` that represents frame start for batch processing. Note that you will get `Sequel::Extensions::Batches::InvalidPKError` in case you provide a hash with wrong keys (ordering matters as well).

### finish

Same as `start` but represents the frame end.

### order

Specifies the primary key order (can be `:asc` or `:desc`). Defaults to `:asc`.

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/umbrellio/sequel-batches.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

<a href="https://github.com/umbrellio/">
<img style="float: left;" src="https://umbrellio.github.io/Umbrellio/supported_by_umbrellio.svg" alt="Supported by Umbrellio" width="439" height="72">
</a>
