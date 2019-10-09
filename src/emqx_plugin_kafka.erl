%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_plugin_kafka).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-export([ load/1
        , unload/0
        ]).

%% Hooks functions
-export([ on_message_publish/2
        ]).

%% Called when the plugin application start
load(Env) ->
    ekaf_init([Env]),
    emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]).

ekaf_init(_Env) ->
    {ok, BrokerValues} = application:get_env(emqx_plugin_kafka, broker),
	EventHost = proplists:get_value(event_host, BrokerValues),
	EventPort = proplists:get_value(event_port, BrokerValues),
	EventPartitionTotal = proplists:get_value(event_partition_total, BrokerValues),
	EventTopic = proplists:get_value(event_topic, BrokerValues),
		
	ets:new(kafka_config, [named_table, protected, set, {keypos, 1}, {read_concurrency,true}]),
	
	ets:insert(kafka_config, {event_host, EventHost}),
	ets:insert(kafka_config, {event_port, EventPort}),
	ets:insert(kafka_config, {event_partition_total, EventPartitionTotal}),
	ets:insert(kafka_config, {event_topic, EventTopic}),
	
    {ok, _} = application:ensure_all_started(gproc),
    {ok, _} = application:ensure_all_started(brod),
	ClientConfig = [{reconnect_cool_down_seconds, 10}, {query_api_versions,false}, {required_acks, none}],
	ok = brod:start_client([{EventHost,EventPort}], event_client,ClientConfig),
	ok = brod:start_producer(event_client, list_to_binary(EventTopic), _ProducerConfig = [{required_acks, none}]).

%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message, _Env) ->
    ?LOG(debug, "on_message_publish msg:~p", [Message]),
	produce_message_kafka_payload(Message),
    {ok, Message}.

timestamp() ->
    {M, S, _} = os:timestamp(),
    M * 1000000 + S.

format_payload(Message) ->
    {ClientId, Username} = format_from(Message#message.from),
    Payload = [{action, <<"message_publish">>},
                  {clientid, ClientId},
                  {username, Username},
                  {topic, Message#message.topic},
                  {payload, Message#message.payload},
                  {ts, timestamp() * 1000}],
    {ok, Payload}.

format_from({ClientId, Username}) ->
    {ClientId, Username};
format_from(From) when is_atom(From) ->
    {a2b(From), a2b(From)};
format_from(_) ->
    {<<>>, <<>>}.

a2b(A) -> erlang:atom_to_binary(A, utf8).

produce_kafka_payload(Message) ->
    % [{_, Topic}] = ets:lookup(topic_table, kafka_payload_topic),
    Topic = <<"sample_topic">>,
	io:format("send to kafka event topic: byte size: ~p~n", [byte_size(list_to_binary(Topic))]),    
    % Payload = iolist_to_binary(mochijson2:encode(Message)),
    Payload = jsx:encode(Message),
    ok = ekaf:produce_async(Topic, Payload),
    % ok = ekaf:produce_async(list_to_binary(Topic), Payload),
    ok.

get_temp_topic(S)->
	case lists:last(S) of
		<<"event">> ->
			<<"">>;
		<<"custom">> ->
			<<"">>;
		Other ->
			Other
	end.

process_message_topic(Topic)->
	{ok, event, Topic}
	

get_proplist_value(Key, Proplist, DefaultValue)->
	case proplists:get_value(Key, Proplist) of
		undefined ->
			DefaultValue;
		Other ->
			Other
	end.

process_message_payload(Payload, TempTopic)->
	case jsx:is_json(Payload) of
		true ->
			BodyResult = jsx:decode(Payload),
			Topic = get_proplist_value(<<"topic">>, BodyResult, <<"">>),
			Action = get_proplist_value(<<"action">>, BodyResult, TempTopic),
			DataResult = proplists:delete(<<"action">>, proplists:delete(<<"topic">>, proplists:delete(<<"timestamp">>, BodyResult))),
			{ok, Topic, Action, DataResult};
		false ->
			{error,"Payload is not a json:"++Payload}
	end.

get_kafka_config(Event, Clientid) ->
	case Event of
		event ->
			[{_, Topic}] = ets:lookup(kafka_config, event_topic),
			[{_, PartitionTotal}] = ets:lookup(kafka_config, event_partition_total),
			Partition = erlang:phash2(Clientid) rem PartitionTotal,
			{ok, list_to_binary(Topic), Partition, event_client};
		Other ->
			?LOG(debug, "unknow envent type:~s",[Other]),
			{error,"unknow envent type:"++Other}
	end.
	

produce_message_kafka_payload(Message) ->
	Topic = Message#message.topic, 
	case process_message_topic(Topic) of 
		{ok, Event, TempTopic} ->
			#{username:=Username} = Message#message.headers,
			case process_message_payload(Message#message.payload, TempTopic) of
				{ok, PaloadTopic, Action, Data} ->
					{M, S, _} = Message#message.timestamp,
					KafkaPayload = [
							{clientId , Message#message.from},
							{recvedAt , timestamp() * 1000},
							{from , <<"mqtt">>},
							{mqttTopic , Topic},
							{topic , PaloadTopic},
							{action , Action},
							{timestamp , (M * 1000000 + S) * 1000},
							{data , Data}
						],
					case get_kafka_config(Event, Message#message.from) of
						{ok, KafkaTopic, Partition, Client} ->
							KafkaMessage = jsx:encode(KafkaPayload),
							?LOG(error,"msg payload: ~s topic:~s", [KafkaMessage, KafkaTopic]),
							case brod:produce(Client, KafkaTopic, Partition, <<>>, KafkaMessage) of
								{ok, CallRef} ->
									?LOG(error,"BROD Returns"),
									brod:sync_produce_request(CallRef);
								{error, Msg} -> 
									?LOG(error, "brod:produce error: ~s",[Msg])
							end;
						{error, Msg} -> 
							?LOG(error, "get_kafka_config error: ~s",[Msg])
					end;
				{error, Msg} ->
					?LOG(error,"msg kafka body error: ~s",[Msg])
			end;
		{error, Msg} ->
			?LOG(error,"process topic error: ~s",[Msg])
	end,
    ok.
	
%% Called when the plugin application stop
unload() ->
    emqx:unhook('client.connected', fun ?MODULE:on_client_connected/4),
    emqx:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
    emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2).
