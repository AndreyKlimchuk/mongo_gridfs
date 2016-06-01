% Licensed under the Apache License, Version 2.0 (the "License"); you may not
% use this file except in compliance with the License. You may obtain a copy of
% the License at
%
%   http://www.apache.org/licenses/LICENSE-2.0
%
% Unless required by applicable law or agreed to in writing, software
% distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
% WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
% License for the specific language governing permissions and limitations under
% the License.

%%% @author CA Meijer
%%% @copyright 2012 CA Meijer
%%% @doc MongoDB GridFS API. This module provides functions for creating, reading, updating and
%%       deleting files from GridFS. The exported functions exposed are similar to the CRUD
%%       functions exposed by the mongo API of the MongoDB driver.
%%% @end

-module(gridfs).


%% Includes
-include("gridfs.hrl").

%% Types

%% API
-export([insert/4, insert/5, insert/6,
  find/2, find/4,
  find_one/2, find_one/4,
  delete_one/2, delete_one/4,
  delete/3, delete/4, delete/2, coll/1]).


%% External functions

%@doc Deletes files matching the selector from the fs.files and fs.chunks collections.
-spec(delete(pid(), bson:document()) -> ok).
delete(Conn, Selector) ->
  delete(Conn, undefined, <<"fs">>, Selector).

%@doc Deletes files matching the selector from the specified bucket.
-spec(delete(pid(), database(), bucket(), bson:document()) -> ok).
delete(Conn, Bucket, Selector) -> delete(Conn, undefined, Bucket, Selector).
delete(Conn, Db, Bucket, Selector) ->
  FilesColl = <<(coll(Bucket))/binary, ".files">>,
  ChunksColl = <<(coll(Bucket))/binary, ".chunks">>,
  Cursor = mc_worker_api:find(Conn, {Db, FilesColl}, Selector, #{projector => #{<<"_id">> => 1}}),
  Files = mc_cursor:rest(Cursor),
  mc_cursor:close(Cursor),
  Ids = [Id || #{<<"_id">> := Id} <- Files],
  mc_worker_api:delete(Conn, {Db, ChunksColl}, {files_id, {'$in', Ids}}),
  mc_worker_api:delete(Conn, {Db, FilesColl}, {'_id', {'$in', Ids}}),
  ok.

%@doc Deletes the first file matching the selector from the fs.files and fs.chunks collections.
-spec(delete_one(pid(), bson:document()) -> ok).
delete_one(Conn, Selector) ->
  delete_one(Conn, undefined, <<"fs">>, Selector).

%@doc Deletes the first file matching the selector from the specified bucket.
-spec(delete_one(pid(), database(), bucket(), bson:document()) -> ok).
delete_one(Conn, Db, Bucket, Selector) ->
  FilesColl = <<(coll(Bucket))/binary, ".files">>,
  ChunksColl = <<(coll(Bucket))/binary, ".chunks">>,
  case mc_worker_api:find_one(Conn, {Db, FilesColl}, Selector, {'_id', 1}) of
    #{<<"_id">> := Id} ->
      mc_worker_api:delete(Conn, {Db, ChunksColl}, {files_id, Id}),
      mc_worker_api:delete_one(Conn, {Db, FilesColl}, {'_id', Id}),
      ok;
    #{} ->
      ok
  end.

%% @doc Executes an 'action' using the specified read and write modes to a database using a connection.
%%      An 'action' is a function that takes no arguments. The fun will usually invoke functions
%%      to do inserts, finds, modifies, deletes, etc.
%%-spec(do(mc_worker_api:write_mode(), mc_worker_api:read_mode(), mc_worker_api:connection()|mc_worker_api:rs_connection(),mc_worker_api:db(), mc_worker_api:action()) -> {ok, any()}|{failure, any()}).
%%do(WriteMode, ReadMode, Connection, Database, Action) ->
%%	%% Since we need to store state information, we spawn a new process for this
%%	%% function so that if the Action also invokes the 'do' function we don't wind up trashing
%%	%% the original state.
%%	ConnectionParameters = #gridfs_connection{write_mode=WriteMode, read_mode=ReadMode, connection=Connection, database=Database},
%%	{ok, Pid} = gen_server:start_link(?MODULE, [ConnectionParameters], []),
%%	gen_server:call(Pid, {do, Action}, infinity).

%@doc Finds the first file matching the selector from the fs.files and fs.chunks collections.
-spec(find_one(pid(), bson:document()) -> file()).
find_one(Conn, Selector) ->
  find_one(Conn, undefined, <<"fs">>, Selector).

%@doc Finds the first file matching the selector from the specified bucket.
-spec(find_one(pid(), database(), bucket(), bson:document()) -> file()).
find_one(Conn, Db, Bucket, Selector) ->
  FilesColl = <<(coll(Bucket))/binary, ".files">>,
  case mc_worker_api:find_one(Conn, {Db, FilesColl}, Selector, #{projector => #{<<"_id">> => 1}}) of
    #{<<"_id">> := Id} ->
      gridfs_file:new(#gridfs_connection{connection = Conn, database = Db}, Bucket, Id, self());
    _ -> error
  end.

%@doc Finds files matching the selector from the fs.files and fs.chunks collections
%     and returns a cursor.
-spec(find(pid(), bson:document()) -> cursor()).
find(Conn, Selector) ->
  find(Conn, undefined, <<"fs">>, Selector).

%@doc Finds files matching the selector from the specified bucket
%     and returns a cursor.
-spec(find(pid(), database(), bucket(), bson:document()) -> cursor()).
find(Conn, Db, Bucket, Selector) ->
  FilesColl = <<(coll(Bucket))/binary, ".files">>,
  MongoCursor = mc_worker_api:find(Conn, {Db, FilesColl}, Selector, #{projector => #{<<"_id">> => 1}}),
  gridfs_cursor:new(#gridfs_connection{connection = Conn, database = Db}, Bucket, MongoCursor, self()).

%@doc Inserts a file with a specified name into the default bucket.
%     The file contents can be passed as either data or a file process opened for
%     reading.
insert(Conn, Db, FileName, FileData) ->
  insert_with_bson(Conn, Db, {filename, FileName}, FileData).

%@doc Inserts a file with a specified bson document into the default bucket.
%     The file contents can be passed as either data or a file process opened for
%     reading.

insert_with_bson(Conn, Db, BsonDocument, FileData) ->
  insert(Conn, Db, <<"fs">>, BsonDocument, FileData).

%@doc Inserts a file with a bson document or filename into the specified bucket.
%     The file contents can be passed as either data or a file process opened for
%     reading.

insert(Conn, Db, Bucket, FileName, FileData) when not is_tuple(FileName) ->
  insert(Conn, Db, Bucket, {filename, FileName}, FileData);
insert(Conn, Db, Bucket, Bson, FileData) when is_binary(FileData) ->
  FilesColl = <<(coll(Bucket))/binary, ".files">>,
  ChunksColl = <<(coll(Bucket))/binary, ".chunks">>,
  ObjectId = mongo_id_server:object_id(),
  insert(Conn, Db, ChunksColl, ObjectId, 0, FileData),
  Md5 = list_to_binary(bin_to_hexstr(crypto:md5(FileData))),
  ListBson = tuple_to_list(Bson),
  ListFileAttr = ['_id', ObjectId, length, size(FileData), chunkSize, ?CHUNK_SIZE, uploadDate, now(), md5, Md5],
  UnifiedList = lists:append([ListFileAttr, ListBson]),
  Res = mc_worker_api:insert(Conn, {Db, FilesColl}, list_to_tuple(UnifiedList)),

  {ok, ObjectId};
insert(Conn, Db, Bucket, Bson, IoStream) ->
  FilesColl = <<(coll(Bucket))/binary, ".files">>,
  ChunksColl = <<(coll(Bucket))/binary, ".chunks">>,
  ObjectId = mongo_id_server:object_id(),
  {Md5, FileSize} = copy(Conn, Db, ChunksColl, ObjectId, 0, IoStream, crypto:md5_init(), 0),
  Md5Str = list_to_binary(bin_to_hexstr(Md5)),
  file:close(IoStream),
  ListBson = tuple_to_list(Bson),
  ListFileAttr = ['_id', ObjectId, length, FileSize, chunkSize, ?CHUNK_SIZE,
    uploadDate, now(), md5, Md5Str],
  UnifiedList = lists:append([ListFileAttr, ListBson]),
  mc_worker_api:insert(Conn, {Db, FilesColl}, list_to_tuple(UnifiedList)),
  {ok, ObjectId}.

%% Internal functions
bin_to_hexstr(Bin) ->
  lists:flatten([io_lib:format("~2.16.0b", [X]) || X <- binary_to_list(Bin)]).

insert(Conn, Db, Coll, ObjectId, N, Data) when size(Data) =< ?CHUNK_SIZE ->
  mc_worker_api:insert(Conn, {Db, Coll}, {'files_id', ObjectId, data, {bin, bin, Data}, n, N});
insert(Conn, Db, Coll, ObjectId, N, Data) ->
  <<Data1:(?CHUNK_SIZE * 8), Data2/binary>> = Data,
  mc_worker_api:insert(Conn, {Db, Coll}, {'files_id', ObjectId, data, {bin, bin, <<Data1:(?CHUNK_SIZE * 8)>>}, n, N}),
  insert(Conn, Db, Coll, ObjectId, N + 1, Data2).

copy(Conn, Db, ChunksColl, ObjectId, N, IoStream, Md5Context, Size) ->
  case file:pread(IoStream, N * ?CHUNK_SIZE, ?CHUNK_SIZE) of
    eof ->
      {crypto:md5_final(Md5Context), Size};
    {ok, Data} ->
      mc_worker_api:insert(Conn, {Db, ChunksColl}, {'files_id', ObjectId, data, {bin, bin, Data}, n, N}),
      copy(Conn, Db, ChunksColl, ObjectId, N + 1, IoStream, crypto:md5_update(Md5Context, Data), Size + size(Data))
  end.

coll(Bucket) when is_atom(Bucket) -> atom_to_binary(Bucket, utf8);
coll(Bucket) when is_binary(Bucket) -> Bucket.
