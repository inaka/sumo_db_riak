%%% @hidden
%%% @doc Riak store implementation.
%%% <u>Implementation Notes:</u>
%%% <ul>
%%% <li> Riak Data Types as main structures to push/pull data.</li>
%%% <li> Bulk operations (such as: delete_all and find_all) were
%%%      optimized using streaming. Records are streamed in portions
%%%      (using Riak 2i to stream keys first), and then the current
%%%      operation (e.g.: delete the record or accumulate the values
%%%      to return them later) is applied. This allows better memory
%%%      and cpu efficiency.</li>
%%% <li> Query functions were implemented using Riak Search on Data Types,
%%%      to get better performance and flexibility.</li>
%%% </ul>
%%%
%%% Copyright 2012 Inaka &lt;hello@inaka.net&gt;
%%%
%%% Licensed under the Apache License, Version 2.0 (the "License");
%%% you may not use this file except in compliance with the License.
%%% You may obtain a copy of the License at
%%%
%%%     http://www.apache.org/licenses/LICENSE-2.0
%%%
%%% Unless required by applicable law or agreed to in writing, software
%%% distributed under the License is distributed on an "AS IS" BASIS,
%%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%%% See the License for the specific language governing permissions and
%%% limitations under the License.
%%% @end
%%% @copyright Inaka <hello@inaka.net>
%%%

-module(sumo_store_riak2i).

-author("Steven Joseph <steven@pointzi.com>").

-github("https://github.com/jagguli").

-license("Apache License 2.0").

-behavior(sumo_store).

-include_lib("riakc/include/riakc.hrl").

%% @todo remove this when riakc releases a new version > 2.5.3
%% They already fixed on master so we should wait until they release a new version

-dialyzer([{nowarn_function, new_doc/2}]).

%% API.

-export(
  [
    init/1,
    create_schema/2,
    persist/2,
    fetch/3,
    delete_by/3,
    delete_all/2,
    find_all/2,
    find_all/5,
    find_by/3,
    find_by/5,
    find_by/6,
    count/2,
    count_by/3,
    index_field/5
  ]
).

%% Utilities

-export(
  [
    %  doc_to_rmap/1,
    %  map_to_rmap/1,
    %  rmap_to_doc/2,
    %  rmap_to_map/2,
    fetch_obj/4
    %  fetch_docs/5,
    %  delete_map/4,
    %  update_map/5,
    %  search/6,
    %  build_query/2
  ]
).

%%%=============================================================================
%%% Types
%%%=============================================================================
%% Riak base parameters

-type connection() :: pid().
-type index() :: binary().
-type options() :: [proplists:property()].

-export_type([connection/0, index/0, options/0]).

%% @doc
%% conn: is the Pid of the gen_server that holds the connection with Riak
%% bucket: Riak bucket (per store)
%% index: Riak index to be used by Riak Search
%% get_opts: Riak read options parameters.
%% put_opts: Riak write options parameters.
%% del_opts: Riak delete options parameters.
%% <a href="http://docs.basho.com/riak/latest/dev/using/basics">Reference</a>.
%% @end

-record(
  state,
  {
    conn :: connection(),
    bucket :: {binary(), binary()},
    index :: index(),
    get_opts :: get_options(),
    put_opts :: put_options(),
    del_opts :: delete_options()
  }
).

-type state() :: #state{}.

%%%=============================================================================
%%% API
%%%=============================================================================

-spec init(term()) -> {ok, term()}.
init(Opts) ->
  % The storage backend key in the options specifies the name of the process
  % which creates and initializes the storage backend.
  Backend = proplists:get_value(storage_backend, Opts),
  Conn = sumo_backend_riak:get_connection(Backend),
  BucketType = sumo_utils:to_bin(sumo_utils:keyfind(bucket_type, Opts)),
  Bucket = sumo_utils:to_bin(sumo_utils:keyfind(bucket, Opts)),
  GetOpts = proplists:get_value(get_options, Opts, []),
  PutOpts = proplists:get_value(put_options, Opts, []),
  DelOpts = proplists:get_value(delete_options, Opts, []),
  State =
    #state{
      conn = Conn,
      bucket = {BucketType, Bucket},
      index = null,
      get_opts = GetOpts,
      put_opts = PutOpts,
      del_opts = DelOpts
    },
  {ok, State}.


-spec persist(Doc, State) ->
  Response
  when Doc :: sumo_internal:doc(),
       State :: state(),
       Response :: sumo_store:result(sumo_internal:doc(), state()).
persist(Doc, #state{conn = Conn, bucket = Bucket, put_opts = Opts} = State) ->
  {Id, NewDoc} = new_doc(sleep(Doc), State),
  try update_obj(Conn, Bucket, Id, NewDoc, Opts) of
    ok -> {ok, wakeup(NewDoc), State};
    Error -> {error, Error, State}
  catch
    Error -> {error, Error, State}
  end.


-spec robj_to_doc(sumo:schema_name(), riakc_obj:riakc_obj()) ->
  sumo_internal:doc().
robj_to_doc(DocName, RObj) -> wakeup(sumo_internal:new_doc(DocName, RObj)).

-spec fetch_obj(Conn, Bucket, Key, Opts) ->
  Result
  when Conn :: connection(),
       Bucket :: bucket_and_type(),
       Key :: key(),
       Opts :: options(),
       Result :: {ok, riakc_obj:riakc_obj()} | {error, term()}.
fetch_obj(Conn, Bucket, Id, Opts) ->
  riakc_pb_socket:get(Conn, Bucket, Id, Opts).

check_resolve_siblings(RiakObject, DocName, Conn) ->
  {riakc_obj, {_BucketType, _Bucket}, _Key, _Context, _, _, _} = RiakObject,
  case riakc_obj:get_contents(RiakObject) of
    [] -> throw(no_value);
    [{_MD, V}] -> V;

    Siblings ->
      Module = sumo_config:get_prop_value(DocName, module),
      {MD, V} = Module:conflict_resolver(Siblings),
      Obj =
        riakc_obj:update_value(riakc_obj:update_metadata(RiakObject, MD), V),
      ok = riakc_pb_socket:put(Conn, Obj),
      V
  end.


-spec fetch(DocName, Id, State) ->
  Response
  when DocName :: sumo:schema_name(),
       Id :: sumo:field_value(),
       State :: state(),
       Response :: sumo_store:result(sumo_internal:doc(), state()).
fetch(
  DocName,
  Id,
  #state{conn = Conn, bucket = Bucket, get_opts = Opts} = State
) ->
  case fetch_obj(Conn, Bucket, sumo_utils:to_bin(Id), Opts) of
    {ok, RiakObject} ->
      {
        ok,
        robj_to_doc(
          DocName,
          jsx:decode(
            check_resolve_siblings(RiakObject, DocName, Conn),
            [{labels, atom}, return_maps]
          )
        ),
        State
      };

    {error, {notfound, _Type = map}} -> {error, notfound, State};
    {error, Error} -> {error, Error, State};
    undef -> {error, undef, State}
  end.


-spec fetch_docs(DocName, Conn, Bucket, Keys, Opts) ->
  Result
  when DocName :: sumo:schema_name(),
       Conn :: connection(),
       Bucket :: bucket_and_type(),
       Keys :: [key()],
       Opts :: options(),
       Result :: [sumo_internal:doc()].
fetch_docs(DocName, Conn, Bucket, Keys, Opts) ->
  lists:foldl(
    fun
      (K, Acc) ->
        case fetch_obj(Conn, Bucket, K, Opts) of
          {ok, M} -> [robj_to_doc(DocName, M) | Acc];
          _ -> Acc
        end
    end,
    [],
    Keys
  ).


-spec delete_obj(connection(), bucket_and_type(), key(), options()) ->
  ok | {error, term()}.
delete_obj(Conn, Bucket, Key, Opts) ->
  riakc_pb_socket:delete(Conn, Bucket, Key, Opts).

-spec delete_by(DocName, Conditions, State) ->
  Response
  when DocName :: sumo:schema_name(),
       Conditions :: sumo:conditions(),
       State :: state(),
       Response :: sumo_store:result(sumo_store:affected_rows(), state()).
delete_by(DocName, Conditions, State) when is_list(Conditions) ->
  #state{conn = Conn, bucket = Bucket, index = _Index, del_opts = Opts} = State,
  IdField = sumo_internal:id_field_name(DocName),
  case lists:keyfind(IdField, 1, Conditions) of
    {_K, Key} ->
      case delete_obj(Conn, Bucket, sumo_utils:to_bin(Key), Opts) of
        ok -> {ok, 1, State};
        {error, Error} -> {error, Error, State}
      end
  end;

delete_by(DocName, Conditions, State) ->
  #state{conn = _Conn, bucket = _Bucket, index = _Index, del_opts = _Opts} =
    State,
  _TranslatedConditions = transform_conditions(DocName, Conditions),
  logger:debug("Conditions ~p", [Conditions]),
  {ok, 0, State}.


%Query = build_query(TranslatedConditions, Bucket),
%case search_keys_by(Conn, Index, Query, [], 0, 0) of
%  {ok, {Total, Res}}  ->
%    delete_keys(Conn, Bucket, Res, Opts),
%    {ok, Total, State};
%  {error, Error} ->
%    {error, Error, State}
%end.
-spec delete_all(DocName, State) ->
  Response
  when DocName :: sumo:schema_name(),
       State :: state(),
       Response :: sumo_store:result(sumo_store:affected_rows(), state()).
delete_all(_DocName, State) ->
  #state{conn = Conn, bucket = Bucket, del_opts = Opts} = State,
  Del =
    fun
      (Kst, Acc) ->
        lists:foreach(fun (K) -> delete_obj(Conn, Bucket, K, Opts) end, Kst),
        Acc + length(Kst)
    end,
  case stream_keys(Conn, Bucket, Del, 0) of
    {ok, Count} -> {ok, Count, State};
    {error, Reason, Count} -> {error, {stream_keys, Reason, Count}, State}
  end.


-spec find_all(DocName, State) ->
  Response
  when DocName :: sumo:schema_name(),
       State :: state(),
       Response :: sumo_store:result([sumo_internal:doc()], state()).
find_all(DocName, State) ->
  #state{conn = Conn, bucket = Bucket, get_opts = Opts} = State,
  Get =
    fun (Kst, Acc) -> fetch_docs(DocName, Conn, Bucket, Kst, Opts) ++ Acc end,
  case stream_keys(Conn, Bucket, Get, []) of
    {ok, Docs} -> {ok, Docs, State};
    {error, Reason, Count} -> {error, {stream_keys, Reason, Count}, State}
  end.


-spec find_all(DocName, Sort, Limit, Offset, State) ->
  Response
  when DocName :: sumo:schema_name(),
       Sort :: term(),
       Limit :: non_neg_integer(),
       Offset :: non_neg_integer(),
       State :: state(),
       Response :: sumo_store:result([sumo_internal:doc()], state()).
find_all(DocName, Sort, Limit, Offset, State) ->
  find_by(DocName, [], Sort, Limit, Offset, State).

%% @doc
%% find_by may be used in two ways: either with a given limit and offset or not
%% If a limit and offset is not given, then the atom 'undefined' is used as a
%% marker to indicate that the store should find out how many keys matching the
%% query exist, and then obtain results for all of them.
%% This is done to overcome Solr's default pagination value of 10.
%% @end

-spec find_by(DocName, Conditions, State) ->
  Response
  when DocName :: sumo:schema_name(),
       Conditions :: sumo:conditions(),
       State :: state(),
       Response :: sumo_store:result([sumo_internal:doc()], state()).
find_by(DocName, Conditions, State) ->
  find_by(DocName, Conditions, undefined, undefined, State).

-spec find_by(DocName, Conditions, Limit, Offset, State) ->
  Response
  when DocName :: sumo:schema_name(),
       Conditions :: sumo:conditions(),
       Limit :: non_neg_integer() | undefined,
       Offset :: non_neg_integer() | undefined,
       State :: state(),
       Response :: sumo_store:result([sumo_internal:doc()], state()).
find_by(_DocName, _Conditions, undefined, undefined, State) -> {ok, [], State};
%% First get all keys matching the query, and then obtain documents for those
%% keys.
%%#state{conn = Conn, bucket = Bucket, index = Index, get_opts = Opts} = State,
%%TranslatedConditions = transform_conditions(DocName, Conditions),
%%Query = build_query(TranslatedConditions, Bucket),
%%case find_by_query_get_keys(Conn, Index, Query) of
%%  {ok, Keys} ->
%%    Results = fetch_docs(DocName, Conn, Bucket, Keys, Opts),
%%    {ok, Results, State};
%%  {error, Error} ->
%%    {error, Error, State}
%%end;
find_by(DocName, Conditions, Limit, Offset, State) ->
  %% Limit and offset were specified so we return a possibly partial result set.
  find_by(DocName, Conditions, [], Limit, Offset, State).


-spec find_by(DocName, Conditions, Sort, Limit, Offset, State) ->
  Response
  when DocName :: sumo:schema_name(),
       Conditions :: sumo:conditions(),
       Sort :: term(),
       Limit :: non_neg_integer(),
       Offset :: non_neg_integer(),
       State :: state(),
       Response :: sumo_store:result([sumo_internal:doc()], state()).
find_by(DocName, Conditions, _Sort, _Limit, _Offset, State) ->
  #state{conn = _Conn, bucket = _Bucket, index = _Index, get_opts = _Opts} =
    State,
  TranslatedConditions = transform_conditions(DocName, Conditions),
  logger:debug("Conditions ~p", [TranslatedConditions]),
  {ok, [], State}.

%%SortOpts = build_sort(Sort),
%%Query = <<(build_query(TranslatedConditions, Bucket))/binary>>,
%%case search_keys_by(Conn, Index, Query, SortOpts, Limit, Offset) of
%%  {ok, {_Total, Keys}} ->
%%    Results = fetch_docs(DocName, Conn, Bucket, Keys, Opts),
%%    {ok, Results, State};
%%  {error, Error} ->
%%    {error, Error, State}
%%end.
%%%=============================================================================
%%% Internal functions
%%%=============================================================================
%% @private

transform_conditions(DocName, Conditions) ->
  sumo_utils:transform_conditions(
    fun validate_date/1,
    DocName,
    Conditions,
    [date, datetime]
  ).

%% @private

validate_date({FieldType, _, FieldValue}) ->
  case {FieldType, sumo_utils:is_datetime(FieldValue)} of
    {datetime, true} -> date_util:datetime_to_epoch(FieldValue);
    {date, true} -> date_util:datetime_to_epoch({FieldValue, {0, 0, 0}})
  end.


sleep(Doc) -> sumo_utils:doc_transform(fun sleep_fun/4, Doc).

%% @private

sleep_fun(_, FieldName, undefined, _) when FieldName /= id -> null;

sleep_fun(FieldType, _, FieldValue, _)
when FieldType =:= datetime and is_binary(FieldValue);
     FieldType =:= date and is_binary(FieldValue) ->
  iso8601:parse(FieldValue);

sleep_fun(FieldType, _, FieldValue, _)
when FieldType =:= datetime; FieldType =:= date ->
  case {FieldType, sumo_utils:is_datetime(FieldValue)} of
    {datetime, true} -> date_util:datetime_to_epoch(FieldValue);
    {date, true} -> date_util:datetime_to_epoch({FieldValue, {0, 0, 0}});
    _ -> date_util:datetime_to_epoch(iso8601:parse(FieldValue))
  end;

sleep_fun(custom, _, FieldValue, FieldAttrs) ->
  Type = sumo_utils:keyfind(type, FieldAttrs, custom),
  sleep_custom(FieldValue, Type);

sleep_fun(_, _, FieldValue, _) -> FieldValue.

%% @private

sleep_custom(FieldValue, FieldType) ->
  case lists:member(FieldType, [term, tuple, map, list]) of
    true -> base64:encode(term_to_binary(FieldValue));
    _ -> FieldValue
  end.

%% @private

wakeup(Doc) -> sumo_utils:doc_transform(fun wakeup_fun/4, Doc).

wakeup_fun(_, _, undefined, _) -> undefined;
wakeup_fun(_, _, <<"$nil">>, _) -> undefined;

wakeup_fun(FieldType, _, FieldValue, _)
when FieldType =:= datetime; FieldType =:= date; is_binary(FieldValue) ->
  iso8601:parse(FieldValue);

wakeup_fun(FieldType, _, FieldValue, _)
when FieldType =:= datetime;
     FieldType =:= date;
     is_float(FieldValue) or is_integer(FieldValue) ->
  date_util:timestamp_to_datetime(FieldValue);

wakeup_fun(integer, _, FieldValue, _) when is_binary(FieldValue) ->
  binary_to_integer(FieldValue);

wakeup_fun(float, _, FieldValue, _) when is_binary(FieldValue) ->
  binary_to_float(FieldValue);

wakeup_fun(boolean, _, FieldValue, _) when is_binary(FieldValue) ->
  binary_to_atom(FieldValue, utf8);

wakeup_fun(custom, _, FieldValue, FieldAttrs) ->
  Type = sumo_utils:keyfind(type, FieldAttrs, custom),
  wakeup_custom(FieldValue, Type);

wakeup_fun(_, _, FieldValue, _) -> FieldValue.

%% @private

wakeup_custom(null, _FieldType) -> null;

wakeup_custom(FieldValue, FieldType) ->
  case lists:member(FieldType, [term, tuple, map, list]) of
    true -> binary_to_term(base64:decode(FieldValue));
    _ -> FieldValue
  end.


-spec index_field(Doc, Key, Value, Type, Accumulator) ->
  Accumulator
  when Doc :: sumo_internal:doc(),
       Key :: atom(),
       Value :: any(),
       Type :: atom(),
       Accumulator :: list().
index_field(_Doc, _Key, null, _, Acc) -> Acc;

index_field(_Doc, Key, Value, integer, Acc) ->
  lists:append(Acc, [{{integer_index, Key}, [Value]}]);

index_field(_Doc, Key, Value, float, Acc) ->
  lists:append(Acc, [{{binary_index, Key}, [float_to_binary(Value)]}]);

index_field(_Doc, Key, Value, datetime, Acc) ->
  lists:append(Acc, [{{binary_index, Key}, [float_to_binary(Value)]}]);

index_field(_Doc, Key, Value, string, Acc) ->
  lists:append(Acc, [{{binary_index, Key}, [Value]}]).

build_index(Doc) ->
  DocName = sumo_internal:doc_name(Doc),
  Schema = sumo_internal:get_schema(DocName),
  SchemaFields = sumo_internal:schema_fields(Schema),
  Module = sumo_config:get_prop_value(DocName, module),
  Index =
    lists:foldl(
      fun
        (Field, Acc) ->
          FieldType = sumo_internal:field_type(Field),
          FieldName = sumo_internal:field_name(Field),
          FieldValue = sumo_internal:get_field(FieldName, Doc),
          %FieldAttrs = sumo_internal:field_attrs(Field),
          Module:index_field(
            maps:get(fields, Doc),
            atom_to_list(FieldName),
            FieldValue,
            FieldType,
            Acc
          )
      end,
      [],
      SchemaFields
    ),
  logger:debug("Build index ~p", [Index]),
  Index.


update_obj(Conn, Bucket, Id, Doc0, _Opts) ->
  Doc =
    maps:filter(
      fun (_K, null) -> false; (_K, _V) -> true end,
      maps:get(fields, Doc0)
    ),
  case fetch_obj(Conn, Bucket, sumo_utils:to_bin(Id), []) of
    {ok, RiakObject} ->
      riakc_obj:update_value(RiakObject, jsx:encode(Doc)),
      MD1 = riakc_obj:get_update_metadata(RiakObject),
      MD2 = riakc_obj:set_secondary_index(MD1, build_index(Doc0)),
      Obj1 = riakc_obj:update_metadata(RiakObject, MD2),
      riakc_pb_socket:put(Conn, Obj1);

    {error, notfound} ->
      RiakObject = riakc_obj:new(Bucket, Id, jsx:encode(Doc)),
      MD1 = riakc_obj:get_update_metadata(RiakObject),
      MD2 = riakc_obj:set_secondary_index(MD1, build_index(Doc0)),
      Obj1 = riakc_obj:update_metadata(RiakObject, MD2),
      riakc_pb_socket:put(Conn, Obj1);

    {error, Error} -> {error, Error};
    undef -> {error, undef}
  end.


%Module = sumo_config:get_prop_value(DocName, module),

%% @private

new_doc(Doc, _State) ->
  DocName = sumo_internal:doc_name(Doc),
  IdField = sumo_internal:id_field_name(DocName),
  Id =
    case sumo_internal:get_field(IdField, Doc) of
      undefined -> throw({error, noid});
      Id0 -> sumo_utils:to_bin(Id0)
    end,
  {Id, sumo_internal:set_field(IdField, Id, Doc)}.


-spec create_schema(Schema, State) ->
  Response
  when Schema :: sumo_internal:schema(),
       State :: state(),
       Response :: sumo_store:result(state()).
create_schema(_Schema, State) -> {ok, State}.

-spec count(DocName, State) ->
  Response
  when DocName :: sumo:schema_name(),
       State :: state(),
       Response :: sumo_store:result(non_neg_integer(), state()).
count(_DocName, #state{conn = Conn, bucket = Bucket} = State) ->
  Sum = fun (Kst, Acc) -> length(Kst) + Acc end,
  case stream_keys(Conn, Bucket, Sum, 0) of
    {ok, Count} -> {ok, Count, State};
    {_, _, _} -> {error, {error, count_failed}, State}
  end.


-spec count_by(DocName, Conditions, State) ->
  Response
  when DocName :: sumo:schema_name(),
       Conditions :: sumo:conditions(),
       State :: state(),
       Response :: sumo_store:result(non_neg_integer(), state()).
count_by(DocName, [], State) -> count(DocName, State);
count_by(_DocName, _Conditions, #{conn := _Conn} = _State) -> 0.

%% @private

stream_keys(Conn, Bucket, F, Acc) ->
  {ok, Ref} =
    riakc_pb_socket:get_index_eq(
      Conn,
      Bucket,
      <<"$bucket">>,
      <<"">>,
      [{stream, true}]
    ),
  receive_stream(Ref, F, Acc).


%stream_index_eq(Conn, Bucket, F, Index, Key, Acc) ->
%  {ok, Ref} =
%    riakc_pb_socket:get_index_eq(Conn, Bucket, Index, Key, [{stream, true}]),
%  receive_stream(Ref, F, Acc).
%
%
%stream_index_range(Conn, Bucket, F, Index, StartKey, EndKey, Acc) ->
%  {ok, Ref} =
%    riakc_pb_socket:get_index_range(
%      Conn,
%      Bucket,
%      Index,
%      StartKey,
%      EndKey,
%      [{stream, true}]
%    ),
%  receive_stream(Ref, F, Acc).

%% @private

receive_stream(Ref, F, Acc) ->
  receive
    {Ref, {_, Keys, _}} -> receive_stream(Ref, F, F(Keys, Acc));
    {Ref, {done, _Continuation = undefined}} -> {ok, Acc};
    Unexpected -> {error, {unexpected, Unexpected}, Acc}
  after
    300000 -> {error, timeout, Acc}
  end.
