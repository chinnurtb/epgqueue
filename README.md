epgqueue
========

epgsql queue

SQL Schema
=========

CREATE TABLE queues
(
  id serial NOT NULL,
  name character varying(40),
  tick integer,
  CONSTRAINT pk_queues_id PRIMARY KEY (id )
);

CREATE TABLE queue_events
(
  evtid serial NOT NULL,
  evtuid integer,
  evtqueue character varying(40),
  evtname character varying(100),
  evtdata text,
  evtstatus boolean,
  evttime timestamp without time zone,
  CONSTRAINT pk_queue_events_id PRIMARY KEY (evtid )
);

App Config
==========

 {epgqueue, [
    {pool, main}
 ]}


API
==========

epgqueue_app:start().

epgqueue:publish(Queue, {Name, Data}).

epgqueue:subscribe(Queue, PID).

epgqueue:unsubscribe(Queue, PID).

Event
=====

pgqevent{name=Name, data=Data, time=Time}


