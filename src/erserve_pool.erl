%%%=============================================================================
%%% @doc Connection pooling for erserve.
%%%=============================================================================
-module(erserve_pool).

-behaviour(application).


%%%_* Exports ------------------------------------------------------------------

%% External API
-export([ get_connection/1
        , get_connection/2
        , return_connection/2
        , start_pool/2
        , stop_pool/1
        ]).

%% application callbacks
-export([ start/2
        , stop/1
        ]).


%%%_* Types --------------------------------------------------------------------
-type name()      :: atom() | string().
-type pool_opts() :: [ proplists:property() ].

-export_type([ name/0
             , pool_opts/0
             ]).


%%%_* External API -------------------------------------------------------------

%%%-----------------------------------------------------------------------------
%%% @doc Get a connection, waiting at most 10 seconds before giving up.
%%%-----------------------------------------------------------------------------
-spec get_connection(name()) -> {ok, erserve:connection()} | error.
get_connection(Pool) ->
  erserve_pool_worker:get_connection(Pool).

%%%-----------------------------------------------------------------------------
%%% @doc Get a connection, waiting at most Timeout seconds before giving up.
%%%-----------------------------------------------------------------------------
-spec get_connection(name(), pos_integer()) -> {ok, erserve:connection()}
                                             | error.
get_connection(Pool, Timeout) ->
  erserve_pool_worker:get_connection(Pool, Timeout).

%%%-----------------------------------------------------------------------------
%%% @doc Return a connection back to the connection pool.
%%%-----------------------------------------------------------------------------
-spec return_connection(name(), erserve:connection()) -> ok.
return_connection(Pool, Conn) ->
  erserve_pool_worker:return_connection(Pool, Conn).

%%%-----------------------------------------------------------------------------
%%% @doc Stop the pool and close all erserve connections.
%%%-----------------------------------------------------------------------------
-spec stop_pool(name()) -> ok.
stop_pool(Pool) ->
  Pid = erserve_pool_worker:get_pid(Pool),
  supervisor:terminate_child(erserve_pool_sup, Pid).

%%%-----------------------------------------------------------------------------
%%% @doc Start a pool called Name. Opts is a proplist that contains the pool
%%%      configuration keys min_size, max_size, max_queue_size and keep_alive,
%%%      and optionally, the keys host and port, giving the hostname and port
%%%      to use with Rserve, respectively. If port or both is empty, the
%%%      defaults from erserve ("localhost", 6311) are used.
%%%-----------------------------------------------------------------------------
-spec start_pool(name(), pool_opts()) -> {ok, pid()} | {error, term()}.
start_pool(Name, Opts0) ->
  Opts = orddict:from_list(Opts0),
  supervisor:start_child(erserve_pool_sup, [Name, Opts]).


%%%_* application callbacks ----------------------------------------------------
-spec start(term(), term()) -> {ok, pid()} | {error, term()}.
start(_StartType, _Args) ->
  supervisor:start_link({local, erserve_pool_sup}, erserve_pool_sup, []).

-spec stop(term()) -> ok.
stop(_State) ->
  ok.
