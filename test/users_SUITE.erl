-module(users_SUITE).

%% CT

-export([all/0, init_per_suite/1, end_per_suite/1, find/1]).

-type config() :: [{atom(), term()}].

%%%=============================================================================
%%% Common Test
%%%=============================================================================

-spec all() -> [atom()].
all() -> [find].

-spec init_per_suite(config()) -> config().
init_per_suite(Config) ->
  {ok, _} = application:ensure_all_started(sumo_db_riak),
  Config.


-spec end_per_suite(config()) -> config().
end_per_suite(Config) ->
  sumo:delete_all(users),
  ok = sumo_db_riak:stop(),
  Config.


-spec find(config()) -> ok.
find(_Config) ->
  Id1 = <<"first">>,
  Id2 = <<"second">>,
  User1 = sumo_test_user:new(Id1, [#{this => is_a_map}, #{this => is_another}]),
  User2 = sumo_test_user:new(Id2, ["A", "B"]),
  sumo:persist(users, User1),
  sumo:persist(users, User2),
  User1 = sumo:fetch(users, Id1),
  User2 = sumo:fetch(users, Id2),
  ok.
