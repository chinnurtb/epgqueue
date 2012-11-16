-module(epgqueue_app).

-export([start/0]).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

start() ->
    epgsql_app:start(),
    application:start(epgqueue).

%% ===================================================================
%% Application callbacks
%% ===================================================================

start(_StartType, _StartArgs) ->
    epgqueue_sup:start_link().

stop(_State) ->
    ok.
