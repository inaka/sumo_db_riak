# sumo_db_riak

[![Build Status](https://travis-ci.org/inaka/sumo_db_riak.svg?branch=master)](https://travis-ci.org/inaka/sumo_db_riak)

This is the [Riak](http://docs.basho.com/riak/latest/) adapter for [sumo_db](https://github.com/inaka/sumo_db).


## Implementation Notes

These are some implementation notes you should know:

 1. This **Riak** adapter is implemented using [Riak Data Types](http://docs.basho.com/riak/latest/dev/using/data-types/).
 2. The modeled **sumo** docs/entities are mapped to [Riak Maps](http://docs.basho.com/riak/latest/dev/using/data-types/#Maps).
 3. This Riak adapter allows nested docs/entities, ang again, those entities are treat them
    as Riak Maps too.
 4. Bulk operations such as: `delete_all/1`, `delete_by/2`, `find_all/1,4` and `find_by/2,4,5`,
    they were optimized using streaming. Records are streamed in chunks (using [Riak 2i](http://docs.basho.com/riak/latest/dev/using/2i/)
    to stream keys first), and then the current operation is applied. E.g: supposing
    we are executing a `find_all/1`, all keys are fetched first, and then the values
    corresponding to each key are fetched (remember that fetch a doc by the key is the
    fastest way, O(1)). This allows better memory and cpu efficiency.
 5. Query functions were implemented using [Riak Search](http://docs.basho.com/riak/latest/dev/search/search-data-types/)
    on Data Types, to get better performance and flexibility.


## Riak

### Install Riak

To install/upgrade **Riak** please follow the instructions in this link:
[Installing and Upgrading Riak](http://docs.basho.com/riak/latest/ops/building/installing).

### Initial Configurations

Due to the fact that **Riak** comes with default configuration, we need to
change some parameters required by `sumo_db`.

**Riak** has a main configuration file `riak.conf`, which you can find into
your installation path `$YOUR_INSTALL_PATH/etc/riak.conf`.

> **Note:** For more information check this link [Configuration Files](http://docs.basho.com/riak/latest/ops/advanced/configs/configuration-files).

First parameter to change is the default **Riak** backend from **Bitcask** to
**LevelDB**. This change also enables the use of [Riak Secondary Indexes](http://docs.basho.com/riak/latest/ops/advanced/configs/secondary-index/).

    storage_backend = leveldb

Then proceed to enable search capabilities:

    search = on

> **Note:** For more information check this link [Riak Search Settings](http://docs.basho.com/riak/latest/ops/advanced/configs/search/).

### Configuring Riak Data Types and Search

First, let's create and activate a bucket type simply called maps that is set up
to store Riak maps:

Because `sumo_db_riak` adapter is implemented using **Riak Data Types**, and docs/entities
are mapped to **Riak Maps**, we need to create a [Riak Bucket Type](http://docs.basho.com/riak/latest/dev/advanced/bucket-types/)
for those docs that will be stored.

Taking as example our [tests](./test), let's call the bucket type `maps`.

    $ riak-admin bucket-type create maps '{"props":{"datatype":"map"}}'
    $ riak-admin bucket-type activate maps

Now, let's create a search index called `sumo_test_index` using the default
schema:

    $ curl -XPUT $RIAK_HOST/search/index/sumo_test_index \
        -H 'Content-Type: application/json' \
        -d '{"schema":"_yz_default"}'

With our index created, we can associate our new `sumo_test_index` index with
our `maps` bucket type:

    $ riak-admin bucket-type update maps '{"props":{"search_index":"sumo_test_index"}}'

Now we can start working with **Riak** from `sumo_db`.

> **Note:** For more information check this link [Riak Data Types and Search](http://docs.basho.com/riak/latest/dev/search/search-data-types/#Maps-Example).


## Getting Started

To start use `sumo_db` with this Riak adapter `sumo_db_riak` is pretty easy, you only has to
follow these steps:

 1. Add `sumo_db_riak` as dependencies in your project.

```erlang
{deps, [
  {sumo_db_riak, "0.1.0"}
]}.
```

 2. You need at least one doc/entity, let's use [sumo_test_people_riak](./test/sumo_test_people_riak.erl)
    as example.

 3. Provide the configuration file, e.g.: [test.config](./test/test.config).

 4. Now you can run your app and start using `sumo` from there.

### Running sumo from an Erlang console

    $ rebar3 shell --config test/test.config

Now you're ready to play with `sumo` and **Riak**:

```erlang
> sumo:find_all(people).
[]
```


## Contact Us
If you find any **bugs** or have a **problem** while using this library, please
[open an issue][issue] in this repo (or a pull request :)).
