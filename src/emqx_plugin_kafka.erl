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
-export([ on_client_connected/4,
          on_client_disconnected/3, 
          on_message_publish/2
        ]).

%% Called when the plugin application start
load(Env) ->
    ekaf_init([Env]),
    emqx:hook('client.connected', fun ?MODULE:on_client_connected/4, [Env]),
    emqx:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]),
    emqx:hook('message.publish', fun ?MODULE:on_message_publish/2, [Env]).

ekaf_init(_Env) ->
    {ok, BrokerValues} = application:get_env(emqx_plugin_kafka, broker),
    KafkaHost = proplists:get_value(host, BrokerValues),
    KafkaPort = proplists:get_value(port, BrokerValues),
    KafkaPartitionStrategy= proplists:get_value(partitionstrategy, BrokerValues),
    KafkaPartitionWorkers= proplists:get_value(partitionworkers, BrokerValues),
    KafkaPayloadTopic = proplists:get_value(payloadtopic, BrokerValues),
    KafkaEventTopic = proplists:get_value(eventtopic, BrokerValues),
    application:set_env(ekaf, ekaf_bootstrap_broker,  {KafkaHost, list_to_integer(KafkaPort)}),
    % application:set_env(ekaf, ekaf_bootstrap_topics,  [<<"Processing">>, <<"DeviceLog">>]),
    application:set_env(ekaf, ekaf_partition_strategy, KafkaPartitionStrategy),
    application:set_env(ekaf, ekaf_per_partition_workers, KafkaPartitionWorkers),
    application:set_env(ekaf, ekaf_per_partition_workers_max, 10),
    % application:set_env(ekaf, ekaf_buffer_ttl, 10),
    % application:set_env(ekaf, ekaf_max_downtime_buffer_size, 5),
    ets:new(topic_table, [named_table, protected, set, {keypos, 1}]),
    ets:insert(topic_table, {kafka_payload_topic, KafkaPayloadTopic}),
    ets:insert(topic_table, {kafka_event_topic, KafkaEventTopic}),
    {ok, _} = application:ensure_all_started(gproc),
    {ok, _} = application:ensure_all_started(ekaf).

on_client_connected(#{clientid := ClientId}, ConnAck, ConnAttrs, _Env) ->
    io:format("Client(~s) connected, connack: ~w, conn_attrs:~p~n", [ClientId, ConnAck, ConnAttrs]).

on_client_disconnected(#{clientid := ClientId}, ReasonCode, _Env) ->
    io:format("Client(~s) disconnected, reason_code: ~w~n", [ClientId, ReasonCode]).

%% Transform message and return
on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};

on_message_publish(Message, _Env) ->
    io:format("Publish ~s~n", [emqx_message:format(Message)]),
%%    produce_message_kafka_payload(Message),
	{ok, Payload} = format_payload(Message),
    produce_kafka_payload(Payload),
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
    [{_, Topic}] = ets:lookup(topic_table, kafka_payload_topic),
    % Topic = <<"Processing">>,
	% io:format("send to kafka event topic: byte size: ~p~n", [byte_size(list_to_binary(Topic))]),    
    % Payload = iolist_to_binary(mochijson2:encode(Message)),
    Payload = jsx:encode(Message),
    % ok = ekaf:produce_async(Topic, Payload),
    ok = ekaf:produce_async(list_to_binary(Topic), Payload),
    ok.

produce_kafka_log(Message) ->
    [{_, Topic}] = ets:lookup(topic_table, kafka_event_topic),
    % Topic = <<"DeviceLog">>,
    % io:format("send to kafka event topic: byte size: ~p~n", [byte_size(list_to_binary(Topic))]),    
    % Payload = iolist_to_binary(mochijson2:encode(Message)),
    Payload = jsx:encode(Message),
    % ok = ekaf:produce_async(Topic, Payload),
    ok = ekaf:produce_async(list_to_binary(Topic), Payload),
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
	S = binary:split(Topic, <<$/>>, [global, trim]),
	Size = array:size(array:from_list(S)),
	if 
		Size>=5 ->
			case lists:nth(5, S) of
				<<"event">> ->
					{ok, event, get_temp_topic(S)};
				<<"custom">> ->
					{ok, custom, get_temp_topic(S)};
				Other ->
					?LOG(debug, "unknow topic:~s event:~p", [Topic, Other]),
					{error,"unknow topic:" ++Topic}
			end;
		true->
			?LOG(debug,"topic size error:~s", [integer_to_list(Size)]),
			{error, "topic size error:"++integer_to_list(Size)}
	end.
	

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
		custom ->
			[{_, Topic}] = ets:lookup(kafka_config, custom_topic),
			[{_, PartitionTotal}] = ets:lookup(kafka_config, custom_partition_total),
			Partition = erlang:phash2(Clientid) rem PartitionTotal,
			{ok, list_to_binary(Topic), Partition, custom_client};
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
							{appId , get_app_id(Username)},
							{recvedAt , timestamp() * 1000},
							{from , <<"mqtt">>},
							{type , <<"string">>},
							{msgId , gen_msg_id(Event)},
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
							{ok, Pid} = brod:produce(Client, KafkaTopic, Partition, <<>>, KafkaMessage);
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

gen_msg_id(connected)->
	list_to_binary("rbc"++string:substr(md5:md5(integer_to_list(timestamp()+rand:uniform(1000000))), 8, 20));

gen_msg_id(disconnected)->
	list_to_binary("rbd"++string:substr(md5:md5(integer_to_list(timestamp()+rand:uniform(1000000))), 8, 20));

gen_msg_id(custom)->
	list_to_binary("rbt"++string:substr(md5:md5(integer_to_list(timestamp()+rand:uniform(1000000))), 8, 20));

gen_msg_id(event)->
	list_to_binary("rbe"++string:substr(md5:md5(integer_to_list(timestamp()+rand:uniform(1000000))), 8, 20)).

get_app_id(Username)->
	if is_binary(Username) ->
		    UsernameStr = binary:bin_to_list(Username);
	   is_list(Username) ->
			UsernameStr = Username;
	   true -> 
		    UsernameStr = ""
	end,
	Position = string:chr(UsernameStr, $@),
	case Position of
		0->	
			<<"">>;
		_->
			list_to_binary(lists:nth(2,string:tokens(UsernameStr,"@")))
	end.

get_mqtt_topic(Clientid, connected)->
	NodeStr = string:concat("$SYS/brokers/", atom_to_list(node())),
	Result =  string:concat(string:concat(string:concat(NodeStr, "/clients/"), binary_to_list(Clientid)), "/connected"),
	list_to_binary(Result);

get_mqtt_topic(Clientid, disconnected)->
	NodeStr = string:concat("$SYS/brokers/", atom_to_list(node())),
	Result =  string:concat(string:concat(string:concat(NodeStr, "/clients/"), binary_to_list(Clientid)), "/disconnected"),
	list_to_binary(Result).

get_ip_str({{I1, I2, I3, I4},_})->
	IP = list_to_binary(integer_to_list(I1)++"."++integer_to_list(I2)++"."++integer_to_list(I3)++"."++integer_to_list(I4)),
	IP.

is_online(connected)->
	true;

is_online(disconnected)->
	false.
	

produce_online_kafka_log(Clientid, Username, Peername, Connection) ->
	Now = timestamp() * 1000,
	MqttTopic = get_mqtt_topic(Clientid, Connection),
	KafkaPayload = [
					{clientId , Clientid},
					{appId , get_app_id(Username)},
					{recvedAt , Now},
					{msgId , gen_msg_id(Connection)},
					{mqttTopic , MqttTopic},
					{deviceSource, <<"roobo">>},
					{from, <<"mqtt">>},
					{action , <<"device.status.online">>},
					{ipaddress , get_ip_str(Peername)},
					{timestamp , Now},
					{isOnline , is_online(Connection)},
					{username , Username}
				],
	[{_,Topic}] = ets:lookup(kafka_config, online_topic),
	[{_, PartitionTotal}] = ets:lookup(kafka_config, online_partition_total),
	Partition = erlang:phash2(Clientid) rem PartitionTotal,
	KafkaMessage = jsx:encode(KafkaPayload),
	?LOG(debug, "~p payload: ~s topic:~s", [Connection, KafkaMessage, Topic]),
	{ok, Pid} = brod:produce(online_client, list_to_binary(Topic), Partition, <<>>, KafkaMessage),
    ok.

%% Called when the plugin application stop
unload() ->
    emqx:unhook('client.connected', fun ?MODULE:on_client_connected/4),
    emqx:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3),
    emqx:unhook('message.publish', fun ?MODULE:on_message_publish/2).
