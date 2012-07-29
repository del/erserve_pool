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
        , start_link/3
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


%%%_* Types --------------------------------------------------------------------
-type seconds() :: pos_integer().


%%%_* Local definitions --------------------------------------------------------
-record(state, { id          :: pid() | string()
               , size        :: pos_integer()
               , connections :: { erserve:connection(), seconds() }
               , monitors    :: [ reference() ]
               , waiting
               , opts
               , timer
               }).


%%%_* External API -------------------------------------------------------------
start_link(Name, Size, Opts) ->
  gen_server:start_link({local, Name}, ?MODULE, {Name, Size, Opts}, []).

%%%-----------------------------------------------------------------------------
%%% @doc Get a connection, waiting at most 10 seconds before giving up.
%%%-----------------------------------------------------------------------------
-spec get_connection(erserve_pool:name()) -> erserve:connection().
get_connection(Pool) ->
  get_connection(Pool, 10000).

%%%-----------------------------------------------------------------------------
%%% @doc Get a connection, waiting at most Timeout seconds before giving up.
%%%-----------------------------------------------------------------------------
-spec get_connection(erserve_pool:name(), pos_integer()) ->
                        erserve:connection().
get_connection(Pool, Timeout) ->
  try
    gen_server:call(Pool, get_connection, Timeout)
  catch
    ErrType:Error ->
      io:format("Error ~p~n~p~n~p~n",
                [ErrType, Error, erlang:get_stacktrace()]),
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
init({Name, Size, Opts}) ->
  process_flag(trap_exit, true),
  Id = case Name of
         undefined -> self();
         _Name     -> Name
       end,
  {ok, Conn} = connect(Opts),
  {ok, TRef} = timer:send_interval(60000, close_unused),
  State = #state{ id          = Id
                , size        = Size
                , opts        = Opts
                , connections = [ {Conn, now_secs()} ]
                , monitors    = []
                , waiting     = queue:new()
                , timer       = TRef
                },
  {ok, State}.

%% Requestor wants a connection. When available then immediately return,
%% otherwise add to the waiting queue.
handle_call(get_connection, From, State=#state{ connections = Connections
                                              , waiting = Waiting         }) ->
  case Connections of
    [ {Conn, _} | Rest] ->
      %% Return existing unused connection
      {noreply, deliver(From, Conn, State#state{connections = Rest})};
    []                  ->
      case length(State#state.monitors) < State#state.size of
        true  ->
          %% Allocate a new connection and return it.
          {ok, Conn} = connect(State#state.opts),
          {noreply, deliver(From, Conn, State)};
        false ->
          %% Reached max connections, let the requestor wait
          {noreply, State#state{waiting = queue:in(From, Waiting)}}
      end
  end;
handle_call(get_pid, _From, State)                                           ->
  {reply, self(), State};
%% Trap unsupported calls
handle_call(Request, _From, State)                                           ->
  {stop, {unsupported_call, Request}, State}.

%% Connection returned from the requestor, back into our pool.
%% Demonitor the requestor.
handle_cast({return_connection, Conn}, State=#state{monitors = Monitors}) ->
  case lists:keytake(Conn, 1, Monitors) of
    {value, {Conn, Monitor}, Monitors2} ->
      erlang:demonitor(Monitor),
      {noreply, return(Conn, State#state{monitors = Monitors2})};
    false                               ->
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
  {noreply, State#state{connections=Used}};
%% Requestor we are monitoring went down. Kill the associated connection,
%% as it might be in an unknown state.
handle_info({'DOWN', Monitor, process, _Pid, _Info},
            State=#state{monitors = Monitors})       ->
  case lists:keytake(Monitor, 2, Monitors) of
    {value, {Conn, Monitor}, Monitors2} ->
      erserve:close(Conn),
      {noreply, State#state{monitors = Monitors2}};
    false                               ->
      {noreply, State}
  end;
%% Connection closed; perform cleanup of monitoring
handle_info({'EXIT', ConnectionPid, _Reason}, State) ->
  #state{ connections = Connections
        , monitors    = Monitors} = State,
  Connections2 = proplists:delete(ConnectionPid, Connections),
  F = fun({Conn, Monitor}) when Conn == ConnectionPid ->
          erlang:demonitor(Monitor),
          false;
         ({_Conn, _Monitor})                          ->
          true
      end,
  Monitors2 = lists:filter(F, Monitors),
  {noreply, State#state{connections = Connections2, monitors = Monitors2}};
%% Trap unsupported info calls.
handle_info(Info, State)                             ->
  {stop, {unsupported_info, Info}, State}.

terminate(_Reason, State) ->
  timer:cancel(State#state.timer),
  ok.

code_change(_OldVsn, State, _Extra) ->
  State.


%%%_* Internal functions -------------------------------------------------------
connect(Opts) ->
  Host = proplists:get_value(host, Opts),
  Port = proplists:get_value(port, Opts),
  Conn = case {Host, Port} of
           {undefined, undefined} -> erserve:open();
           {_SomeHost, undefined} -> erserve:open(Host);
           {_SomeHost, _SomePort} -> erserve:open(Host, Port)
         end,
  case run_init_commands(Conn, Opts) of
    ok    -> {ok, Conn};
    Error -> Error
  end.

run_init_commands(Conn, Opts) ->
  case proplists:get_value(init_commands, Opts) of
    undefined ->
      ok;
    []        ->
      ok;
    Commands  ->
      try
        lists:foreach(fun(Cmd) ->
                          ok = erserve:eval_void(Conn, Cmd)
                      end, Commands),
        ok
      catch
        _:Error ->
          {error, init_commands, Error}
      end
  end.

deliver(From={Pid, _Tag}, Conn, State=#state{monitors=Monitors}) ->
  Monitor  = erlang:monitor(process, Pid),
  gen_server:reply(From, {ok, Conn}),
  State#state{monitors = [ {Conn, Monitor} | Monitors ]}.

return(Conn, State=#state{ connections = Connections
                         , waiting     = Waiting     }) ->
  case queue:out(Waiting) of
    { {value, From}, Waiting2 } ->
      State2 = deliver(From, Conn, State),
      State2#state{waiting = Waiting2};
    {empty, _Waiting} ->
      Connections2 = [ {Conn, now_secs()} | Connections ],
      State#state{connections = Connections2}
  end.


%% Return the current time in seconds, used for timeouts.
now_secs() ->
  {MegaSecs, Secs, _MilliSecs} = erlang:now(),
  MegaSecs * 1000 + Secs.
