%%% @hidden
%%% @doc Riak storage backend implementation.
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

-module(sumo_backend_riak).

-author("Carlos Andres Bolanos <candres.bolanos@inakanetworks.com>").

-license("Apache License 2.0").

-behaviour(gen_server).
-behaviour(sumo_backend).

%%% API

-export([start_link/2, get_connection/1]).

%%% Exports for gen_server

-export(
  [
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
  ]
).

%%%=============================================================================
%%% Types
%%%=============================================================================

-record(state, {host :: string(), port :: non_neg_integer(), opts :: [term()]}).

-type state() :: #state{}.

%%%=============================================================================
%%% API
%%%=============================================================================

-spec start_link(atom(), proplists:proplist()) -> {ok, pid()} | term().
start_link(Name, Options) ->
  gen_server:start_link({local, Name}, ?MODULE, Options, []).

-spec get_connection(atom() | pid()) -> pid().
get_connection(Name) -> gen_server:call(Name, get_connection).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

-spec init([term()]) -> {ok, state()}.
init(Options) ->
  %% Get connection parameters
  Host = proplists:get_value(host, Options, "127.0.0.1"),
  Port = proplists:get_value(port, Options, 8087),
  Opts = riak_opts(Options),
  {ok, #state{host = Host, port = Port, opts = Opts}}.

%% @todo: implement connection pool.
%% In other cases is a built-in feature of the client.

-spec handle_call(term(), term(), state()) -> {reply, term(), state()}.
handle_call(
  get_connection,
  _From,
  State = #state{host = Host, port = Port, opts = Opts}
) ->
  {ok, Conn} = riakc_pb_socket:start_link(Host, Port, Opts),
  {reply, Conn, State}.

%%%=============================================================================
%%% gen_server unused callbacks
%%%=============================================================================

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Msg, State) -> {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(_Msg, State) -> {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) -> ok.

-spec code_change(term(), state(), term()) -> {ok, state()}.
code_change(_OldVsn, State, _Extra) -> {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

-spec riak_opts([term()]) -> [term()].
riak_opts(Options) ->
  User = proplists:get_value(username, Options),
  Pass = proplists:get_value(password, Options),
  Opts0 =
    case User /= undefined andalso Pass /= undefined of
      true -> [{credentials, User, Pass}];
      _ -> []
    end,
  case lists:keyfind(connect_timeout, 1, Options) of
    {_, V1} -> [{connect_timeout, V1}, {auto_reconnect, true}] ++ Opts0;
    _ -> [{auto_reconnect, true}] ++ Opts0
  end.
