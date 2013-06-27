%%%=============================================================================
%%% @doc The gen_server that handles a connection pool. It will open connections
%%%      on request, until a maximum number of connections has been reached. At
%%%      that point, requestors will be placed in a queue with a timeout to wait
%%%      for a connection to become free.
%%%      The processes that receive connections are monitored, so connections
%%%      are released back into the pool if the using process dies.
%%%=============================================================================
-module(erserve_pool_worker).


%%%_* Exports ------------------------------------------------------------------
%% External API
-export([ get_connection/1
        , get_connection/2
        , get_pid/1
        , return_connection/2
        , start_link/2
        , stop/1
        ]).

%% gen_server callbacks
-export([ code_change/3
        , handle_call/3
        , handle_cast/2
        , handle_info/2
        , init/1
        , terminate/2
        ]).


%%%_* Local definitions --------------------------------------------------------
-record(state, { id             :: pid() | string()
               , min_size       :: pos_integer()
               , max_size       :: pos_integer()
               , max_queue_size :: pos_integer() | inf
               , connections    :: [ tagged_connection() ]
               , monitors       :: [ {tagged_connection(), reference()} ]
               , waiting        :: [ pid() ]
               , opts           :: options()
               , timer          :: timer:tref()
               }).


%%%_* Types --------------------------------------------------------------------
-type options()           :: orddict:orddict().
-type seconds()           :: pos_integer().
-type state()             :: #state{}.
-type tagged_connection() :: { erserve:connection(), seconds() }.

-export_type([ options/0
             ]).


%%%_* External API -------------------------------------------------------------
-spec start_link(erserve_pool:name(), options()) ->
                    {ok, pid()}
                  | {error, {already_started, pid()}}
                  | {error, Reason :: term()}.
start_link(Name, Opts) ->
  gen_server:start_link({local, Name}, ?MODULE, {Name, Opts}, []).

%%%-----------------------------------------------------------------------------
%%% @doc Get a connection, waiting at most 10 seconds before giving up.
%%%-----------------------------------------------------------------------------
-spec get_connection(erserve_pool:name()) -> {ok, erserve:connection()} | error.
get_connection(Pool) ->
  get_connection(Pool, 10000).

%%%-----------------------------------------------------------------------------
%%% @doc Get a connection, waiting at most Timeout seconds before giving up.
%%%-----------------------------------------------------------------------------
-spec get_connection(erserve_pool:name(), pos_integer()) ->
                        {ok, erserve:connection()} | error.
get_connection(Pool, Timeout) ->
  try
    gen_server:call(Pool, get_connection, Timeout)
  catch
    ErrType:Error ->
      lager:warning( "erserve_pool | failed to get connection ~p ~p"
                   , [ErrType, Error] ),
      gen_server:cast(Pool, {cancel_wait, self()}),
      {error, timeout}
  end.

%%%-----------------------------------------------------------------------------
%%% @doc Return a connection back to the connection pool.
%%%-----------------------------------------------------------------------------
-spec return_connection(erserve_pool:name(), erserve:connection()) -> ok.
return_connection(Pool, Conn) ->
    gen_server:cast(Pool, {return_connection, Conn}).

%%%-----------------------------------------------------------------------------
%%% @doc Get the pid of a pool.
%%%-----------------------------------------------------------------------------
-spec get_pid(erserve_pool:name()) -> pid().
get_pid(Pool) ->
  gen_server:call(Pool, get_pid).

%%%-----------------------------------------------------------------------------
%%% @doc Stop the pool and close all erserve connections.
%%%-----------------------------------------------------------------------------
-spec stop(erserve_pool:name()) -> ok.
stop(Pool) ->
  gen_server:cast(Pool, stop).


%%%_* gen_server callbacks -----------------------------------------------------
-spec init({erserve_pool:name(), options()}) -> {ok, state()}.
init({Name, Opts}) ->
  process_flag(trap_exit, true),
  Id           = case Name of
                   undefined -> self();
                   _Name     -> Name
                 end,
  assert_config(Opts),
  MinSize      = orddict:fetch(min_size,       Opts),
  MaxSize      = orddict:fetch(max_size,       Opts),
  MaxQueueSize = orddict:fetch(max_queue_size, Opts),
  KeepAlive    = orddict:fetch(keep_alive,     Opts),
  State = #state{ id             = Id
                , min_size       = MinSize
                , max_size       = MaxSize
                , max_queue_size = MaxQueueSize
                , opts           = Opts
                , connections    = []
                , monitors       = []
                , waiting        = queue:new()
                , timer          = maybe_close_unused_timer(Opts)
                },
  lager:debug( "erserve_pool | initialising pool, name ~p min size ~p "
               "max size ~p max queue size ~p keep_alive ~p"
             , [Id, MinSize, MaxSize, MaxQueueSize, KeepAlive] ),
  %% Send message to self to begin initialising connections
  gen_server:cast(self(), {start_connections, MinSize}),
  {ok, State}.

-spec assert_config(options()) -> ok.
assert_config(Opts) ->
  MandatoryKeys = [min_size, max_size, max_queue_size, keep_alive],
  lists:foreach(fun(Key) ->
                    case orddict:find(Key, Opts) of
                      {ok, _Value} -> ok;
                      error        -> throw({missing_config_key, Key})
                    end
                end, MandatoryKeys),
  ok.

maybe_close_unused_timer(Opts) ->
  case orddict:fetch(keep_alive, Opts) of
    true ->
      undefined;
    _    ->
      {ok, TRef} = timer:send_interval(60000, close_unused),
      TRef
  end.

%% Request for a connection. If one is available, return it. Otherwise, we may
%% start a new request, and also may queue the requestor.
handle_call(get_connection, From, State=#state{connections = Connections}) ->
  NConnections = length(Connections),
  case Connections of
    [{Conn, _} | Rest] ->
      %% Return existing unused connection
      lager:debug("erserve_pool | connection available"),
      {noreply, deliver(From, Conn, State#state{connections = Rest})};
    []                 ->
      lager:debug("erserve_pool | no connection available"),
      %% If there's room in the pool, trigger opening a new connection
      case NConnections < State#state.max_size of
        true  -> spawn_connect(State#state.opts);
        false -> ok
      end,
      %% If there's room in the queue, let the requestor wait.
      case maybe_queue(From, State) of
        {queued,   NewState} ->
          lager:debug("erserve_pool | queueing requestor"),
          {noreply, NewState};
        {rejected, NewState} ->
          lager:debug("erserve_pool | rejecting connection request"),
          {reply, error, NewState}
      end
  end;
handle_call(get_pid, _From, State)                                         ->
  {reply, self(), State};
%% Trap unsupported calls
handle_call(Request, _From, State)                                         ->
  {stop, {unsupported_call, Request}, State}.

-spec maybe_queue(term(), state()) -> {queued, state()} | {rejected, state()}.
maybe_queue(From, State = #state{ waiting        = Waiting
                                , max_queue_size = MaxSize }) ->
  NWaiting = queue:len(Waiting),
  case NWaiting < MaxSize of
    true  -> {queued,   State#state{ waiting = queue:in(From, Waiting) }};
    false -> {rejected, State}
  end.

%% Open a number of connections
handle_cast({start_connections, N}, State=#state{opts = Opts})            ->
  lager:debug("erserve_pool | starting ~p connections", [N]),
  spawn_connect(Opts, N),
  {noreply, State};
%% New connection finished initialising and ready to be added to pool.
handle_cast({new_connection, Conn}, State)                                ->
  lager:debug("erserve_pool | new connection received, adding to pool"),
  {noreply, return(Conn, State)};
%% Connection returned from the requestor, back into our pool.
%% Demonitor the requestor.
handle_cast({return_connection, Conn}, State=#state{monitors = Monitors}) ->
  lager:debug("erserve_pool | connection returned, adding to pool"),
  case lists:keytake(Conn, 1, Monitors) of
    {value, {Conn, Monitor}, Monitors2} ->
      erlang:demonitor(Monitor),
      {noreply, return(Conn, State#state{monitors = Monitors2})};
    false                               ->
      lager:debug("erserve_pool | failed to demonitor on returned connection"),
      {noreply, State}
  end;
%% Requestor gave up (timeout), remove from our waiting queue (if any).
handle_cast({cancel_wait, Pid}, State=#state{waiting = Waiting})          ->
  Waiting2 = queue:filter(fun({QPid, _Tag}) ->
                              QPid =/= Pid
                          end, Waiting),
  {noreply, State#state{waiting = Waiting2}};
%% Stop the connections pool.
handle_cast(stop, State)                                                  ->
  {stop, normal, State};
%% Trap unsupported casts
handle_cast(Request, State)                                               ->
  {stop, {unsupported_cast, Request}, State}.

%% Close all connections that are unused for longer than a minute.
handle_info(close_unused, State)                     ->
  Old            = now_secs() - 60,
  {Unused, Used} = lists:partition(fun({_Conn, Time}) ->
                                       Time < Old
                                   end, State#state.connections),
  [ erserve:close(Conn) || {Conn, _} <- Unused ],
  lager:debug( "erserve_pool | closed ~p unused connections"
             , [erlang:length(Unused)] ),
  {noreply, State#state{connections=Used}};
%% Requestor we are monitoring went down. Kill the associated connection,
%% as it might be in an unknown state.
handle_info({'DOWN', Monitor, process, _Pid, _Info},
            State=#state{monitors = Monitors})       ->
  lager:warning( "erserve_pool | process holding connection crashed, "
                 "killing connection" ),
  case lists:keytake(Monitor, 2, Monitors) of
    {value, {Conn, Monitor}, Monitors2} ->
      erserve:close(Conn),
      {noreply, State#state{monitors = Monitors2}};
    false                               ->
      {noreply, State}
  end;
%% Connection closed; perform cleanup of monitoring
handle_info({'EXIT', ConnectionPid, _Reason}, State) ->
  lager:debug("erserve_pool | connection closed, clean up monitoring"),
  #state{ connections = Connections0
        , monitors    = Monitors0} = State,
  Connections = proplists:delete(ConnectionPid, Connections0),
  F = fun({{Pid, _Time}, Monitor}) when Pid == ConnectionPid ->
          erlang:demonitor(Monitor),
          false;
         ({_Conn, _Monitor})                                 ->
          true
      end,
  Monitors = lists:filter(F, Monitors0),
  {noreply, State#state{connections = Connections, monitors = Monitors}};
%% Trap unsupported info calls.
handle_info(Info, State)                             ->
  {stop, {unsupported_info, Info}, State}.

terminate(_Reason, State) ->
  lager:debug("erserve_pool | pool terminating"),
  case State#state.timer of
    undefined -> ok;
    TRef      -> timer:cancel(TRef)
  end,
  ok.

code_change(_OldVsn, State, _Extra) ->
  State.


%%%_* Internal functions -------------------------------------------------------
-spec spawn_connect(options()) -> ok.
spawn_connect(Opts) ->
  spawn_connect(Opts, 1).

-spec spawn_connect(options(), pos_integer()) -> ok.
spawn_connect(_Opts, 0)                           -> ok;
spawn_connect( Opts, N) when is_integer(N), N > 0 ->
  PoolPid = self(),
  F       = fun() ->
                {ok, Conn} = connect(Opts),
                erserve:controlling_process(Conn, PoolPid),
                gen_server:cast(PoolPid, {new_connection, Conn})
            end,
  proc_lib:spawn(F),
  spawn_connect(Opts, N-1).

connect(Opts) ->
  lager:debug("erserve_pool | initialising new worker"),
  Host = case orddict:find(host, Opts) of
           {ok, Host0} -> Host0;
           error       -> undefined
         end,
  Port = case orddict:find(port, Opts) of
           {ok, Port0} -> Port0;
           error       -> undefined
         end,
  Conn = case {Host, Port} of
           {undefined, undefined} -> erserve:open();
           {_SomeHost, undefined} -> erserve:open(Host);
           {_SomeHost, _SomePort} -> erserve:open(Host, Port)
         end,
  case run_init_commands(Conn, Opts) of
    ok    ->
      lager:debug("erserve_pool | new worker successfully initialised"),
      {ok, Conn};
    Error ->
      lager:warning("erserve_pool | init command failed ~p", [Error]),
      Error
  end.

run_init_commands(Conn, Opts) ->
  case orddict:find(init_commands, Opts) of
    error          -> ok;
    {ok, []}       -> ok;
    {ok, Commands} ->
      try
        lager:debug( "erserve_pool | running ~p init commands",
                     [length(Commands)] ),
        lists:foreach(fun(Cmd) ->
                          ok = erserve:eval_void(Conn, Cmd)
                      end, Commands),
        ok = erserve:eval_void(Conn, "1")
      catch
        _:Error ->
          {error, init_commands, Error}
      end
  end.

deliver(From={Pid, _Tag}, Conn, State=#state{monitors=Monitors}) ->
  Monitor = erlang:monitor(process, Pid),
  gen_server:reply(From, {ok, Conn}),
  State#state{monitors = [ {Conn, Monitor} | Monitors ]}.

return(Conn, State=#state{ connections = Connections
                         , waiting     = Waiting }) ->
  case queue:out(Waiting) of
    { {value, From}, Waiting2 } ->
      State2 = deliver(From, Conn, State),
      State2#state{waiting = Waiting2};
    {empty, _Waiting} ->
      Connections2 = [ {Conn, now_secs()} | Connections ],
      State#state{connections = Connections2}
  end.


%% Return the current time in seconds, used for timeouts.
-spec now_secs() -> seconds().
now_secs() ->
  {MegaSecs, Secs, _MilliSecs} = erlang:now(),
  MegaSecs * 1000 + Secs.
