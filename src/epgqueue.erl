%%%----------------------------------------------------------------------
%%% File    : epgqueue.erl
%%% Author  : Ery Lee <ery.lee@gmail.com>
%%% Purpose : 
%%% Created : 16 Dec. 2012
%%% License : http://www.opengoss.com
%%%
%%% Copyright (C) 2012, www.opengoss.com
%%%----------------------------------------------------------------------
-module(epgqueue).

-import(proplists, [get_value/2,
                    get_value/3]).

-include("pgqueue.hrl").

-export([start_link/0,
        publish/2,
        subscribe/2,
        unsubscribe/2,
        clear/1]).

-behavior(gen_server).

-export([init/1,
        handle_call/3,
        handle_cast/2,
        handle_info/2,
        terminate/2,
        code_change/3]).

-record(state, {pool, subscribers=[]}).

-record(subscriber, {spid, queue, ref}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

publish(Queue, {Name, Data}) when is_atom(Queue) ->
    gen_server:cast(?MODULE, {publish, Queue, {Name, Data}}).

subscribe(Queue, Pid) when is_atom(Queue) and is_pid(Pid) ->
    gen_server:call(?MODULE, {subscribe, Queue, Pid}).

unsubscribe(Queue, Pid) when is_atom(Queue) and is_pid(Pid) ->
    gen_server:call(?MODULE, {unsubscribe, Queue, Pid}).

clear(Queue) when is_atom(Queue) ->
    gen_server:call(?MODULE, {clear, Queue}).

init([]) ->
    {ok, Pool} = application:get_env(pool),
    %clear all events first
    epgsql:delete(Pool, queue_events),
    %load all queues
    {ok, Queues} = epgsql:select(Pool, queues),
    %begin to tick
    lists:foreach(fun(Q) ->
        Name = list_to_atom(binary_to_list(get_value(name, Q))),
        Tick = get_value(tick, Q, 10),
        erlang:send_after(Tick*1000, self(), {tick, Tick, Name})
    end, Queues),
    {ok, #state{pool = Pool, subscribers=[]}}.

handle_call({subscribe, Queue, Pid}, _From, 
    #state{subscribers=Subscribers} = State) ->
    Ref = erlang:monitor(process, Pid),
    Sub = #subscriber{spid = Pid, queue = Queue, ref = Ref},
    {reply, ok, State#state{subscribers=[Sub|Subscribers]}};

handle_call({unsubscribe, Queue, Pid}, _From,
    #state{subscribers=Subscribers} = State) ->
    case findone({Queue, Pid}, Subscribers) of
    {ok, Sub} ->
        erlang:demonitor(Sub#subscriber.ref),
        Subscribers1 = lists:delete(Sub, Subscribers),
        {reply, ok, State#state{subscribers=Subscribers1}};
    false ->
        {reply, false, State}
    end;

handle_call({clear, Queue}, _From, #state{pool=Pool}=State) ->
    eqgsql:delete(Pool, queue_events, {evtqueue, Queue}),
    {reply, ok, State};

handle_call(Req, _From, State) ->
    {reply, {error, {badreq, Req}}, State}.

handle_cast({publish, Queue, {Name, Data}}, #state{pool=Pool}=State) ->
    Now = {datetime, date(), time()},
    Record = [{evtqueue, Queue}, {evtname, Name},
              {evtdata, Data}, {evtstatus, false}, 
              {evttime, Now}],
    epgsql:insert(Pool, queue_events, Record),
    {noreply, State};
    
handle_cast(Msg, State) ->
    {stop, {error, {badmsg, Msg}}, State}.

%%--------------------------------------------------------------------
%% Function: handle_info(Info, State) -> {noreply, State} |
%%                                       {noreply, State, Timeout} |
%%                                       {stop, Reason, State}
%% Description: Handling all non call/cast messages
%%--------------------------------------------------------------------
handle_info({tick, Tick, Queue}, #state{pool = Pool, subscribers = Subscribers}=State) ->
    %io:format("~p tick~n", [Queue]),
    SubsOfQueue = find(Queue, Subscribers),
    {ok, Events} = epgsql:select(Pool, queue_events, {'and', {evtqueue, Queue}, {evtstatus, false}}),
    Ids =
    lists:map(fun(Event) -> 
        notify(SubsOfQueue, #pgqevent{
            name=get_value(evtname, Event),
            data=get_value(evtdata, Event),
            time=get_value(evttime, Event)}),
        get_value(evtid, Event)
    end, Events),
    epgsql:update(Pool, queue_events, [{evtstatus, true}], {'in', evtid, Ids}),
    erlang:send_after(Tick*1000, self(), {tick, Tick, Queue}),
    {noreply, State};

handle_info({'DOWN', Ref, _Type, _Object, _Info}, 
    #state{subscribers=Subscribers}=State) ->
    Subscribers1 = remove(Ref, Subscribers),
    {noreply, State#state{subscribers=Subscribers1}};

handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% Function: terminate(Reason, State) -> void()
%% Description: This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any necessary
%% cleaning up. When it returns, the gen_server terminates with Reason.
%% The return value is ignored.
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.
%%--------------------------------------------------------------------
%% Func: code_change(OldVsn, State, Extra) -> {ok, NewState}
%% Description: Convert process state when code is changed
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%%% Internal functions
%%--------------------------------------------------------------------
findone({_Queue, _Pid}, []) ->
    false;
findone({Queue, Pid}, [#subscriber{queue=Queue, spid=Pid}=Sub|_]) ->
    {ok, Sub};
findone({Queue, Pid}, [_H|Subscribers]) ->
    findone({Queue, Pid}, Subscribers).

find(Queue, Subscribers) ->
    find(Queue, Subscribers, []).
find(_Queue, [], Acc) ->
    Acc;
find(Queue, [#subscriber{queue=Queue}=Sub|Subscribers], Acc) ->
    find(Queue, Subscribers, [Sub|Acc]).

notify(Subscribers, Event) ->
    [Pid ! Event || #subscriber{spid = Pid} <- Subscribers].

remove(Ref, Subscribers) ->
    remove(Ref, Subscribers, []).

remove(_Ref, [], Acc) ->
    Acc;
remove(Ref, [#subscriber{ref=Ref}|T], Acc) ->
    remove(Ref, T, Acc);
remove(Ref, [H|T], Acc) ->
    remove(Ref, T, [H|Acc]). 

