-module(erserve_pool_sup).

-behaviour(supervisor).


%%%_* Exports ------------------------------------------------------------------
%% supervisor callbacks
-export([ init/1
        ]).


%%%_* supervisor callbacks -----------------------------------------------------
-spec init([]) -> term().
init([]) ->
  { ok
  , { { simple_one_for_one, 2, 60 }
    , [ { pool
        , { erserve_pool_worker, start_link, [] }
        , permanent
        , 2000
        , supervisor
        , [ erserve_pool_worker ]
        }
      ]
    }
  }.
