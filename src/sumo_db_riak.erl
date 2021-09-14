-module(sumo_db_riak).

-behaviour(application).

%% API

-export([start/0, stop/0]).

%% Application callbacks

-export([start/2, stop/1]).

%%%=============================================================================
%%% API
%%%=============================================================================

-spec start() -> {ok, _} | {error, term()}.
start() -> application:ensure_all_started(sumo_db_riak).

-spec stop() -> ok | {error, term()}.
stop() -> application:stop(sumo_db_riak).

%%%=============================================================================
%%% Application callbacks
%%%=============================================================================

-spec start(StartType, StartArgs) ->
  Response
  when StartType :: application:start_type(),
       StartArgs :: term(),
       State :: term(),
       Reason :: term(),
       Response :: {ok, pid()} | {ok, pid(), State} | {error, Reason}.
start(_StartType, _StartArgs) -> {ok, self()}.

-spec stop(State :: term()) -> term().
stop(_State) -> ok.
