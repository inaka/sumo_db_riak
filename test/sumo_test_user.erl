-module(sumo_test_user).

-behaviour(sumo_doc).

-type user() :: #{id         => binary(),
                  attributes => list()}.

%% API
-export([
  new/2,
  id/1,
  attributes/1
]).

%% sumo_doc callbacks
-export([sumo_schema/0, sumo_wakeup/1, sumo_sleep/1]).

%%%=============================================================================
%%% sumo_doc callbacks
%%%=============================================================================

-spec sumo_schema() -> sumo:schema().
sumo_schema() ->
  sumo:new_schema(users, [
    sumo:new_field(id,         string, [id, not_null]),
    sumo:new_field(attributes, custom, [{type, list}])
  ]).

-spec sumo_sleep(user()) -> sumo:model().
sumo_sleep(User) ->
  User.

-spec sumo_wakeup(sumo:model()) -> user().
sumo_wakeup(Doc) ->
  Doc.

%%%=============================================================================
%%% API
%%%=============================================================================

-spec new(binary(), list()) -> user().
new(Id, Attributes) ->
  #{id          => Id,
    attributes  => Attributes}.

-spec id(user()) -> string().
id(#{id := Val}) ->
  Val.

-spec attributes(user()) -> list().
attributes(#{attributes := Val}) ->
  Val.
