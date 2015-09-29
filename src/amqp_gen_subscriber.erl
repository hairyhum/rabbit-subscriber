-module(amqp_gen_subscriber).
-export([start_link/4, stop/1]).
-behaviour(gen_fsm).
-include_lib("amqp_client/include/amqp_client.hrl").

-export([
  init/1, 
  handle_event/3, 
  handle_info/3, 
  code_change/4, 
  terminate/3, 
  handle_sync_event/4
]).

-export([
  consume/2,
  deliver/2,
  consume_stopping/2,
  stopping/2]).

-record(state, {
  inner_state :: term(),
  channel :: pid(),
  callback_module :: atom(),
  consumer_tag :: term()
}).

-spec start_link(Channel::pid(), #'basic.consume'{}, atom(), term()) -> 
  {ok, Subscriber::pid()} | ignore | {error, term()}.
-spec stop(Subscriber::pid()) -> ok.

-callback init(Args::term()) -> 
  {ok, State::term()} | ignore | {stop, Reason::term()}.
-callback handle_deliver(BasicDeliver::#'basic.deliver'{}, State::term()) -> 
  ack | reject.
-callback handle_info(Message::term(), State::term()) -> 
  {noreply, NewState::term()} | {stop, Reason::term(), NewState::term()}.
-callback terminate(Reason::term(), State::term()) -> term().

start_link(Channel, Consume, Module, Args) ->
  gen_fsm:start_link(?MODULE, {Module, Channel, Consume, Args}, []).

stop(Pid) -> gen_fsm:sync_send_all_state_event(Pid, stop).

init({Module, Channel, Consume, Args}) ->
  case Module:init(Args) of
    {ok, InnerState} -> 
      amqp_channel:call(Channel, #'basic.qos'{prefetch_count = 1}),
      amqp_channel:subscribe(Channel, Consume, self()),
      {ok, consume, #state{ 
        inner_state = InnerState, 
        callback_module = Module,
        channel = Channel
        }};
    ignore -> ignore;
    {stop, _Reason} = Err -> Err
  end.

consume(#'basic.consume_ok'{consumer_tag = Tag}, State) ->
  {next_state, deliver, State#state{ consumer_tag = Tag }}.

deliver(
    #'basic.deliver'{delivery_tag = Tag} = Deliver, 
    #state{ 
      channel = Channel, 
      inner_state = InnerState, 
      callback_module = Module } = State) ->
  case Module:handle_deliver(Deliver, InnerState) of
    ack ->
      amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag});
    reject ->
      amqp_channel:cast(Channel, #'basic.reject'{delivery_tag = Tag})
  end,
  {next_state, deliver, State}.

consume_stopping(#'basic.consume_ok'{consumer_tag = Tag}, State) ->
  cancel(stopped, State#state{ consumer_tag = Tag}).

stopping(#'basic.cancel_ok'{ consumer_tag = Tag }, {Reason, State}) ->
   #state{ consumer_tag = Tag } = State,
  {stop, Reason, State}.

handle_event(stop, Consume, State) 
    when Consume == consume; Consume == consume_stopping ->
  {next_state, consume_stopping, State};
handle_event(stop, _, State) ->
  cancel(stopped, State).

handle_sync_event(_Event, _From, StateName, State) ->
  error_logger:error_msg("Unknown sync event ~p on state ~p", [_Event, StateName]),
  {reply, ok, StateName, State}.

handle_info(Msg, StateName, #state{ 
    callback_module = Module, 
    inner_state = InnerState } = State) ->
  case Module:handle_info(Msg, InnerState) of
    {noreply, NewInnerState} ->
      {next_state, StateName, update_inner_state(State, NewInnerState)};
    {stop, Reason, NewInnerState} ->
      cancel(Reason, update_inner_state(State, NewInnerState))      
  end.


-spec cancel(Reason, State) -> 
  {next_state, stopping, {Reason, State}}
  when Reason::term(), State::#state{}.
cancel(Reason, #state{
      channel = Channel, 
      consumer_tag = Tag
      } = State) ->
  amqp_channel:cast(Channel, #'basic.cancel'{ consumer_tag = Tag }),
  {next_state, stopping, {Reason, State}}.

terminate(Reason, _StateName, #state{ 
    callback_module = Module, 
    inner_state = InnerState }) ->
  Module:terminate(Reason, InnerState).

code_change(
      OldVsn, 
      StateName, 
      #state{ callback_module = Module, inner_state = InnerState } = State,
      Extra) -> 
  {ok, NewInnerState} = Module:code_change(OldVsn, InnerState, Extra),
  {ok, StateName, update_inner_state(State, NewInnerState)}.

update_inner_state(#state{} = State, NewInnerState) ->
  State#state{ inner_state = NewInnerState }.

  