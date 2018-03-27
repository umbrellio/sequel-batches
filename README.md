# Sequel::Batches [![Build Status](https://travis-ci.org/umbrellio/sequel-batches.svg?branch=master)](https://travis-ci.org/umbrellio/sequel-batches) [![Coverage Status](https://coveralls.io/repos/github/umbrellio/sequel-batches/badge.svg?branch=master)](https://coveralls.io/github/umbrellio/sequel-batches?branch=master)

This dataset extension provides the method #in_batches. The method splits dataset in parts and yields it.

You can set following options:
  - pk Overrides primary key of your dataset
  - of sets chunk size (1000 by default)
  - start as a hash { [column]: <start_value> } represents frame start for batch processing
  - finish as a hash represents frame end

## Installation

Add this line to your application's Gemfile:

```ruby
gem 'sequel-batches'
```

And then execute:

    $ bundle

Or install it yourself as:

    $ gem install sequel-batches

## Usage

In order to use the feature you should enable the extension

```ruby
Sequel::DATABASES.first.extension :batches
```

And then the method becomes available on dataset

```ruby
User.where(role: "admin").in_batches(of: 4) do |ds|
  ds.delete
end
```

Finally, here's an example including all the available options

```ruby
Event.where(type: "login").in_batches(of: 4, pk: [:project_id, :external_user_id], start: { project_id: 2, external_user_id: 3 }, finish: { project_id: 5, external_user_id: 70 }) do |ds|
  ds.delete
end
```

## Development

After checking out the repo, run `bin/setup` to install dependencies. Then, run `rake spec` to run the tests. You can also run `bin/console` for an interactive prompt that will allow you to experiment.

To install this gem onto your local machine, run `bundle exec rake install`. To release a new version, update the version number in `version.rb`, and then run `bundle exec rake release`, which will create a git tag for the version, push git commits and tags, and push the `.gem` file to [rubygems.org](https://rubygems.org).

## Contributing

Bug reports and pull requests are welcome on GitHub at https://github.com/[USERNAME]/sequel-batches. This project is intended to be a safe, welcoming space for collaboration, and contributors are expected to adhere to the [Contributor Covenant](http://contributor-covenant.org) code of conduct.

## License

The gem is available as open source under the terms of the [MIT License](https://opensource.org/licenses/MIT).

## Code of Conduct

Everyone interacting in the Sequel::Batches project’s codebases, issue trackers, chat rooms and mailing lists is expected to follow the [code of conduct](https://github.com/[USERNAME]/sequel-batches/blob/master/CODE_OF_CONDUCT.md).
