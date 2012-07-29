# erserve_pool - Connection pooling for erserve

## Introduction

### erserve

erserve is an Erlang application that implements the communication
with Rserve, making it possible to make calls to R from Erlang, and to
receive data back.

### erserve_pool

erserve_pool provides a simple connection pool for erserve, including
a fixed set of commands to be run upon opening a connection.

erserve_pool monitors the process which called get_connection and
returns the allocated connection to the pool if that process dies. If
a connection dies, a new one is created and added to the pool in its
place.

## Example

% Start a pool
NConnections = 10,
Opts         = [ {host, "localhost"}
               , {port, 6311}
               , {init_commands, ["library(plyr)"]}
               ],
erserve_pool:start_pool(my_pool, NConnections, Opts),

% Get a connection
{ok, Conn} = erserve_pool:get_connection(my_pool),

% Run some erserve commands
erserve:eval_void(Conn, "c(1,2,3)"),

% Return the connection to the pool
erserve_pool:return_connection(my_pool, Conn).